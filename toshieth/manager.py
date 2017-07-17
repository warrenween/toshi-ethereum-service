import asyncio
import logging

from toshi.database import DatabaseMixin
from toshi.redis import RedisMixin
from toshieth.mixins import BalanceMixin
from toshi.ethereum.mixin import EthereumMixin
from toshi.jsonrpc.errors import JsonRPCError
from toshi.log import configure_logger
from toshi.utils import parse_int
from toshi.tasks import TaskHandler, TaskDispatcher
from toshi.sofa import SofaPayment
from toshi.ethereum.tx import (
    create_transaction, add_signature_to_transaction, encode_transaction
)
from toshi.ethereum.utils import data_decoder, data_encoder

from toshieth.tasks import TaskListenerApplication

log = logging.getLogger("toshieth.manager")

class TransactionQueueHandler(DatabaseMixin, RedisMixin, EthereumMixin, BalanceMixin, TaskHandler):

    @property
    def tasks(self):
        if not hasattr(self, '_task_dispatcter'):
            self._task_dispatcter = TaskDispatcher(self.listener)
        return self._task_dispatcter

    async def process_transaction_queue(self, ethereum_address):

        # make sure we only run one check per address at a time
        if ethereum_address not in self.listener.processing_queue:
            self.listener.processing_queue[ethereum_address] = asyncio.Queue()
        else:
            f = asyncio.Future()
            self.listener.processing_queue[ethereum_address].put_nowait(f)
            await f

        try:
            await self._process_transaction_queue(ethereum_address)
        except:
            log.exception("Unexpected issue calling process transaction queue")
        finally:
            if self.listener.processing_queue[ethereum_address].empty():
                del self.listener.processing_queue[ethereum_address]
                # if we didn't process the queue completely
                # then schedule it again

            else:
                f = self.listener.processing_queue[ethereum_address].get_nowait()
                f.set_result(True)

    async def _process_transaction_queue(self, ethereum_address):

        log.info("processing tx queue for {}".format(ethereum_address))

        # check for un-scheduled transactions
        async with self.db:
            # get the last block number to use in ethereum calls
            # to avoid race conditions in transactions being confirmed
            # on the network before the block monitor sees and updates them in the database
            last_blocknumber = (await self.db.fetchval("SELECT blocknumber FROM last_blocknumber"))
            transactions_out = await self.db.fetch(
                "SELECT * FROM transactions "
                "WHERE from_address = $1 "
                "AND (status is NULL OR status = 'queued') "
                "AND r IS NOT NULL "
                # order by nonce reversed so that .pop() can
                # be used in the loop below
                "ORDER BY nonce DESC",
                ethereum_address)

        # any time the state of a transaction is changed we need to make
        # sure those changes cascade down to the receiving address as well
        # this keeps a list of all the receiving addresses that need to be
        # checked after the current address's queue has been processed
        addresses_to_check = set()

        if transactions_out:

            # TODO: make sure the block number isn't too far apart from the current
            # if this is the case then we should just come back later!

            # get the current network balance for this address
            balance = await self.eth.eth_getBalance(ethereum_address, block=last_blocknumber or "latest")

            # get the unconfirmed_txs
            async with self.db:
                unconfirmed_txs = await self.db.fetch(
                    "SELECT nonce, value, gas, gas_price FROM transactions "
                    "WHERE from_address = $1 "
                    "AND (status = 'unconfirmed' "
                    "OR (status = 'confirmed' AND blocknumber > $2)) "
                    "ORDER BY nonce",
                    ethereum_address, last_blocknumber or 0)

            if unconfirmed_txs:
                nonce = unconfirmed_txs[-1]['nonce'] + 1
                balance -= sum(parse_int(tx['value']) + (parse_int(tx['gas']) * parse_int(tx['gas_price'])) for tx in unconfirmed_txs)
            else:
                # use the nonce from the network
                nonce = await self.eth.eth_getTransactionCount(ethereum_address, block=last_blocknumber or "latest")

            # marker for whether a previous transaction had an error (signaling
            # that all the following should also be an error
            previous_error = False

            # for each one, check if we can schedule them yet
            while transactions_out:
                transaction = transactions_out.pop()

                # if there was a previous error in the queue, abort!
                if previous_error:
                    log.info("Setting tx '{}' to error due to previous error".format(transaction['hash']))
                    await self.update_transaction(transaction['transaction_id'], 'error')
                    addresses_to_check.add(transaction['to_address'])
                    continue

                # make sure the nonce is still valid
                if nonce != transaction['nonce']:
                    # then this and all the following transactions are now invalid
                    previous_error = True
                    log.info("Setting tx '{}' to error due to the nonce not matching the network".format(transaction['hash']))
                    await self.update_transaction(transaction['transaction_id'], 'error')
                    addresses_to_check.add(transaction['to_address'])
                    continue

                value = parse_int(transaction['value'])
                gas = parse_int(transaction['gas'])
                gas_price = parse_int(transaction['gas_price'])
                cost = value + (gas * gas_price)

                # check if the current balance is high enough to send to the network
                if balance >= cost:
                    # if so, send the transaction
                    # create the transaction
                    data = data_decoder(transaction['data']) if transaction['data'] else b''
                    tx = create_transaction(nonce=nonce, value=value, gasprice=gas_price, startgas=gas,
                                            to=transaction['to_address'], data=data,
                                            v=parse_int(transaction['v']),
                                            r=parse_int(transaction['r']),
                                            s=parse_int(transaction['s']))
                    # make sure the signature was valid
                    if data_encoder(tx.sender) != ethereum_address:
                        # signature is invalid for the user
                        log.error("ERROR signature invalid for sender of tx: {}".format(transaction['hash']))
                        log.error("queue: {}, db: {}, tx: {}".format(ethereum_address, transaction['from_address'], data_encoder(tx.sender)))
                        previous_error = True
                        addresses_to_check.add(transaction['to_address'])
                        await self.update_transaction(transaction['transaction_id'], 'error')
                        continue
                    # send the transaction
                    try:
                        tx_encoded = encode_transaction(tx)
                        await self.eth.eth_sendRawTransaction(tx_encoded)
                        await self.update_transaction(transaction['transaction_id'], 'unconfirmed')
                    except JsonRPCError as e:
                        # if something goes wrong with sending the transaction
                        # simply abort for now.
                        # TODO: depending on error, just break and queue to retry later
                        log.error("ERROR sending queued transaction: {}".format(e.format()))
                        previous_error = True
                        await self.update_transaction(transaction['transaction_id'], 'error')
                        addresses_to_check.add(transaction['to_address'])
                        continue

                    # adjust the balance values for checking the other transactions
                    balance -= cost
                    nonce += 1
                    continue
                else:
                    # make sure the pending_balance would support this transaction
                    # otherwise there's no way this transaction will be able to
                    # be send, so trigger a failure on all the remaining transactions

                    async with self.db:
                        transactions_in = await self.db.fetch(
                            "SELECT * FROM transactions "
                            "WHERE to_address = $1 "
                            "AND ("
                            "(status is NULL OR status = 'queued' OR status = 'unconfirmed') "
                            "OR (status = 'confirmed' AND blocknumber > $2))",
                            ethereum_address, last_blocknumber or 0)

                    # TODO: test if loops in the queue chain are problematic
                    pending_received = sum((parse_int(p['value']) or 0) for p in transactions_in)

                    if balance + pending_received < cost:
                        previous_error = True
                        log.info("Setting tx '{}' to error due to insufficient pending balance".format(transaction['hash']))
                        await self.update_transaction(transaction['transaction_id'], 'error')
                        addresses_to_check.add(transaction['to_address'])
                        continue
                    else:
                        if any(t['blocknumber'] is not None and t['blocknumber'] > last_blocknumber for t in transactions_in):
                            addresses_to_check.add(ethereum_address)

                        # there's no reason to continue on here since all the
                        # following transaction in the queue cannot be processed
                        # until this one is

                        # but we still need to send PNs for any "new" transactions
                        while transaction:
                            if transaction['status'] is None:
                                await self.update_transaction(transaction['transaction_id'], 'queued')
                            transaction = transactions_out.pop() if transactions_out else None
                        break

        for address in addresses_to_check:
            # make sure we don't try process any contract deployments
            if address != "0x":
                self.tasks.process_transaction_queue(address)

        if transactions_out:
            self.tasks.process_transaction_queue(ethereum_address)

    async def update_transaction(self, transaction_id, status):

        async with self.db:
            tx = await self.db.fetchrow("SELECT * FROM transactions WHERE transaction_id = $1", transaction_id)
            if tx is None or tx['status'] == status:
                return

            # check if we're trying to update the state of a tx that is already confirmed, we have an issue
            if tx['status'] == 'confirmed':
                log.warning("Trying to update status of tx {} to error, but tx is already confirmed".format(tx['hash']))
                return

            log.info("Updating status of tx {} to {} (previously: {})".format(tx['hash'], status, tx['status']))

            if status == 'confirmed':
                blocknumber = parse_int((await self.eth.eth_getTransactionByHash(tx['hash']))['blockNumber'])
                await self.db.execute("UPDATE transactions SET status = $1, blocknumber = $2, updated = (now() AT TIME ZONE 'utc') "
                                      "WHERE transaction_id = $3",
                                      status, blocknumber, transaction_id)
            else:
                await self.db.execute("UPDATE transactions SET status = $1, updated = (now() AT TIME ZONE 'utc') WHERE transaction_id = $2",
                                      status, transaction_id)
            await self.db.commit()

        # render notification

        # don't send "queued"
        if status == 'queued':
            status = 'unconfirmed'
        elif status == 'unconfirmed' and tx['status'] == 'queued':
            # there's already been a tx for this so no need to send another
            return

        payment = SofaPayment(value=parse_int(tx['value']), txHash=tx['hash'], status=status,
                              fromAddress=tx['from_address'], toAddress=tx['to_address'],
                              networkId=self.application.config['ethereum']['network_id'])
        message = payment.render()

        # figure out what addresses need pns
        # from address always needs a pn
        self.tasks.send_notification(tx['from_address'], message)

        # no need to check to_address for contract deployments
        if tx['to_address'] == "0x":
            return

        # check if this is a brand new tx with no status
        if tx['status'] is None:
            # if an error has happened before any PNs have been sent
            # we only need to send the error to the sender, thus we
            # only add 'to' if the new status is not an error
            if status != 'error':
                self.tasks.send_notification(tx['to_address'], message)
        else:
            self.tasks.send_notification(tx['to_address'], message)

        # trigger a processing of the to_address's queue incase it has
        # things waiting on this transaction
        self.tasks.process_transaction_queue(tx['to_address'])

    async def sanity_check(self, frequency):
        async with self.db:
            rows = await self.db.fetch(
                "SELECT DISTINCT from_address FROM transactions WHERE (status = 'unconfirmed' OR status = 'queued' OR status IS NULL) "
                "AND created < (now() AT TIME ZONE 'utc') - interval '2 minutes'"
            )
        if rows:
            log.info("sanity check found {} addresses with potential problematic transactions".format(len(rows)))

        addresses_to_check = set()

        for row in rows:

            ethereum_address = row['from_address']

            # check on unconfirmed transactions
            async with self.db:
                unconfirmed_transactions = await self.db.fetch(
                    "SELECT * FROM transactions "
                    "WHERE from_address = $1 "
                    "AND status = 'unconfirmed'",
                    ethereum_address)

            if len(unconfirmed_transactions) > 0:

                for transaction in unconfirmed_transactions:

                    # check on unconfirmed transactions first
                    if transaction['status'] == 'unconfirmed':
                        # we need to check the true status of unconfirmed transactions
                        # as the block monitor may be inbetween calls and not have seen
                        # this transaction to mark it as confirmed.
                        tx = await self.eth.eth_getTransactionByHash(transaction['hash'])

                        # sanity check to make sure the tx still exists
                        if tx is None:
                            # if not, set to error!
                            log.info("Setting unconfirmed tx '{}' to error as it is no longer visible on the node".format(transaction['hash']))
                            await self.update_transaction(transaction['transaction_id'], 'error')
                            addresses_to_check.add(transaction['from_address'])
                            addresses_to_check.add(transaction['to_address'])

                        elif tx['blockNumber'] is not None:
                            # confirmed! update the status
                            await self.update_transaction(transaction['transaction_id'], 'confirmed')
                            addresses_to_check.add(transaction['from_address'])
                            addresses_to_check.add(transaction['to_address'])

                        else:

                            log.warning("WARNING: transaction '{}' is on the node that is old and unconfirmed".format(transaction['hash']))

            else:

                log.error("ERROR: {} has transactions in it's queue, but no unconfirmed transactions!")
                # trigger queue processing as last resort
                addresses_to_check.add(ethereum_address)

        for address in addresses_to_check:
            # make sure we don't try process any contract deployments
            if address != "0x":
                self.tasks.process_transaction_queue(address)

        if frequency:
            self.tasks.sanity_check(frequency, delay=frequency)

class TaskManager(TaskListenerApplication):

    def __init__(self, *args, **kwargs):
        super().__init__([(TransactionQueueHandler,)], *args, listener_id="manager", **kwargs)
        configure_logger(log)
        self.task_listener.processing_queue = {}

    def start(self):
        # XXX: delay 10 so the redis connection is active before
        # it gets called.. this shouldn't matter
        self.task_listener.call_task('sanity_check', 60, delay=10)
        return super().start()

if __name__ == "__main__":
    app = TaskManager()
    app.run()
