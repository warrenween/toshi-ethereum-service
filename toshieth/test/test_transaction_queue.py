import asyncio
import os
from tornado.escape import json_decode
from tornado.testing import gen_test

from toshieth.test.base import EthServiceBaseTest, requires_full_stack
from toshi.test.ethereum.parity import FAUCET_PRIVATE_KEY, FAUCET_ADDRESS
from toshi.test.ethereum.faucet import FaucetMixin
from toshi.ethereum.utils import data_decoder, data_encoder, private_key_to_address
from toshi.ethereum.tx import sign_transaction, DEFAULT_STARTGAS, DEFAULT_GASPRICE
from toshi.utils import parse_int
from toshi.jsonrpc.client import JsonRPCClient

TEST_PRIVATE_KEY_1 = data_decoder("0xe8f32e723decf4051aefac8e2c93c9c5b214313817cdb01a1494b917c8436b35")
TEST_ADDRESS_1 = "0x056db290f8ba3250ca64a45d16284d04bc6f5fbf"

TEST_PRIVATE_KEY_2 = data_decoder("0x0ffdb88a7a0a40831ca0b19bd31f3f6085764ef8b7db1bd6b57072e5eaea24ff")
TEST_ADDRESS_2 = "0x35351b44e03ec8515664a955146bf9c6e503a381"

TEST_PRIVATE_KEY_3 = data_decoder("0xbc91668394b936efc05aecbf30213746ce7967374918ba75d0bc751a30463cd5")
TEST_ADDRESS_3 = "0x4aaddc8923dfbe6053291c8ba785af5bda49d905"

TEST_PRIVATE_KEY_4 = data_decoder("0x3d60b77d9bd1e63775e4666baeda12d1d236265a85a5a4b620bcbe47ca7bcde4")
TEST_ADDRESS_4 = "0xc4867418f9950f1c32f79fee9098f9058c06823c"

TEST_PRIVATE_KEY_5 = data_decoder("0x40ba61d699250a190ec87e78be117007c7acbacb2c18c88f649c31dfc28bfb2a")
TEST_ADDRESS_5 = "0x3fa28bbf036821db8342f66b7ce8ff583e0ebfd5"

TEST_PRIVATE_KEY_6 = data_decoder("0x006876936f5bf4f404a080e474f94aa3fcd7c413009533fcde51e951abb7f724")
TEST_ADDRESS_6 = "0xb33bcd070ed7c5effa861051a1ec25b818fea111"

def with_timeout(func=None, timeout=5):
    def wrap(fn):
        async def wrapper(self, *args, **kwargs):
            xtimeout = kwargs.pop('timeout', timeout)
            result = await asyncio.wait_for(fn(self, *args, **kwargs), timeout=xtimeout)
            return result
        return wrapper
    if func is not None:
        return wrap(func)
    else:
        return wrap

class TransactionQueueTest(EthServiceBaseTest):

    @gen_test(timeout=30)
    @requires_full_stack(ethminer=True)
    async def test_send_transaction_with_unconfirmed_funds(self, *, ethminer):
        """Tests that someone can create a tx with unconfirmed funds"""

        # make sure no blocks are mined
        ethminer.pause()

        # send funds to address1
        val1 = 10 ** 18
        resp = await self.fetch("/tx/skel", method="POST", body={
            "from": FAUCET_ADDRESS,
            "to": TEST_ADDRESS_1,
            "value": val1  # 1 eth
        })
        self.assertEqual(resp.code, 200)
        body = json_decode(resp.body)
        tx = sign_transaction(body['tx'], FAUCET_PRIVATE_KEY)
        resp = await self.fetch("/tx", method="POST", body={
            "tx": tx
        })
        self.assertEqual(resp.code, 200, resp.body)
        tx1_hash = json_decode(resp.body)['tx_hash']

        # send transaction from address1
        val2 = 2 * 10 ** 17  # 0.2 eth
        resp = await self.fetch("/tx/skel", method="POST", body={
            "from": TEST_ADDRESS_1,
            "to": TEST_ADDRESS_2,
            "value": val2
        })
        self.assertEqual(resp.code, 200)
        body = json_decode(resp.body)
        tx = sign_transaction(body['tx'], TEST_PRIVATE_KEY_1)
        resp = await self.fetch("/tx", method="POST", body={
            "tx": tx
        })
        self.assertEqual(resp.code, 200, resp.body)
        tx2_hash = json_decode(resp.body)['tx_hash']

        await asyncio.sleep(1)

        # make sure 2nd transaction is 'queued'
        async with self.pool.acquire() as con:
            row = await con.fetchrow("SELECT * FROM transactions WHERE from_address = $1", TEST_ADDRESS_1)
        self.assertIsNotNone(row)
        self.assertEqual(row['status'], 'queued')

        # make sure balance adjusts for queued items
        resp = await self.fetch('/balance/{}'.format(TEST_ADDRESS_1))
        self.assertEqual(resp.code, 200)
        data = json_decode(resp.body)
        self.assertEqual(parse_int(data['confirmed_balance']), 0)
        self.assertEqual(parse_int(data['unconfirmed_balance']), val1 - (val2 + (DEFAULT_STARTGAS * DEFAULT_GASPRICE)))

        ethminer.start()

        await self.wait_on_tx_confirmation(tx1_hash)
        await self.wait_on_tx_confirmation(tx2_hash)

        await asyncio.sleep(1)

    async def ensure_confirmed(self, *tx_hashes, raise_error_instead_of_waiting=False, poll_frequency=1):
        where_q = " OR ".join("hash = ${}".format(i) for i in range(1, len(tx_hashes) + 1))
        c_query = "SELECT COUNT(*) FROM transactions WHERE ({}) AND status = 'confirmed'".format(where_q)
        e_query = "SELECT COUNT(*) FROM transactions WHERE ({}) AND status = 'error'".format(where_q)
        while True:
            async with self.pool.acquire() as con:
                result = await con.fetchval(c_query, *tx_hashes)
                errors = await con.fetchval(e_query, *tx_hashes)
            self.assertEqual(errors, 0, "some transactions had errors")
            if result == len(tx_hashes):
                return True
            if raise_error_instead_of_waiting:
                self.assertEqual(result, len(tx_hashes))
            await asyncio.sleep(poll_frequency)

    @with_timeout(timeout=10)
    async def ensure_errors(self, *tx_hashes, poll_frequency=1):
        where_q = " OR ".join("hash = ${}".format(i) for i in range(1, len(tx_hashes) + 1))
        e_query = "SELECT COUNT(*) FROM transactions WHERE ({}) AND status = 'error'".format(where_q)
        while True:
            async with self.pool.acquire() as con:
                errors = await con.fetchval(e_query, *tx_hashes)
            if errors == len(tx_hashes):
                return True
            await asyncio.sleep(poll_frequency)

    @gen_test(timeout=60)
    @requires_full_stack(ethminer=True)
    async def test_long_tx_queue_chain(self, *, ethminer):
        """Tests that a long chain of txs depending on a single initial tx go through"""

        # make sure no blocks are mined
        ethminer.pause()

        val1 = 1000 ** 18
        # send funds to address1
        tx1_hash = await self.send_tx(FAUCET_PRIVATE_KEY, TEST_ADDRESS_1, val1)

        val2 = 100 ** 18
        # send funds from address 1 to address 2
        tx2_hash = await self.send_tx(TEST_PRIVATE_KEY_1, TEST_ADDRESS_2, val2)

        # send funds from address 1 to address 3
        tx3_hash = await self.send_tx(TEST_PRIVATE_KEY_1, TEST_ADDRESS_3, val2)

        val3 = 10 ** 18
        # send funds from address 2 to address 4
        tx4_hash = await self.send_tx(TEST_PRIVATE_KEY_2, TEST_ADDRESS_4, val3)
        # send funds from address 3 to address 4
        tx5_hash = await self.send_tx(TEST_PRIVATE_KEY_3, TEST_ADDRESS_4, val3)

        val4 = int(val3 + (val3 / 2))
        # send funds from address 4 to address 5 that depends on both transactions
        tx6_hash = await self.send_tx(TEST_PRIVATE_KEY_4, TEST_ADDRESS_5, val4)

        # send funds from address 5 to address 6 that depends on both transactions
        tx7_hash = await self.send_tx(TEST_PRIVATE_KEY_5, TEST_ADDRESS_6, val3)

        ethminer.start()

        await self.ensure_confirmed(tx1_hash,
                                    tx2_hash,
                                    tx3_hash,
                                    tx4_hash,
                                    tx5_hash,
                                    tx6_hash,
                                    tx7_hash)

    @gen_test(timeout=300)
    @requires_full_stack(ethminer=True)
    async def test_tx_queue_ping_pong(self, *, ethminer):
        """Tests that funds can be ping ponged between accounts"""

        # make sure no blocks are mined
        ethminer.pause()

        default_fees = DEFAULT_STARTGAS * DEFAULT_GASPRICE

        val = 1000 ** 18
        txs = []
        # send funds to address1
        tx_hash = await self.send_tx(FAUCET_PRIVATE_KEY, TEST_ADDRESS_1, val)
        txs.append(tx_hash)

        addr1, pk1 = TEST_ADDRESS_1, TEST_PRIVATE_KEY_1
        addr2, pk2 = TEST_ADDRESS_2, TEST_PRIVATE_KEY_2
        for i in range(10):
            val = val - default_fees
            # send funds
            tx_hash = await self.send_tx(pk1, addr2, val)
            txs.append(tx_hash)
            # swap all the variables
            addr1, pk1, addr2, pk2 = addr2, pk2, addr1, pk1

        ethminer.start()

        await self.ensure_confirmed(*txs)

    @gen_test(timeout=300)
    @requires_full_stack(ethminer=True)
    async def test_tx_queue_cycle(self, *, ethminer):
        """Tests that a loop of txs around many addresses go through"""

        # make sure no blocks are mined
        ethminer.pause()

        default_fees = DEFAULT_STARTGAS * DEFAULT_GASPRICE

        val = 1000 ** 18
        txs = []
        # send funds to address1
        tx_hash = await self.send_tx(FAUCET_PRIVATE_KEY, TEST_ADDRESS_1, val)
        txs.append(tx_hash)

        addresses = [(TEST_ADDRESS_1, TEST_PRIVATE_KEY_1),
                     (TEST_ADDRESS_2, TEST_PRIVATE_KEY_2),
                     (TEST_ADDRESS_3, TEST_PRIVATE_KEY_3),
                     (TEST_ADDRESS_4, TEST_PRIVATE_KEY_4),
                     (TEST_ADDRESS_5, TEST_PRIVATE_KEY_5),
                     (TEST_ADDRESS_6, TEST_PRIVATE_KEY_6)]

        for i in range(10):
            val = val - default_fees
            addr1, pk1 = addresses[0]
            addr2, pk2 = addresses[1]
            # send funds
            tx_hash = await self.send_tx(pk1, addr2, val)
            txs.append(tx_hash)
            # swap all the variables
            addresses = addresses[1:] + [addresses[0]]

        ethminer.start()

        await self.ensure_confirmed(*txs)

    @gen_test(timeout=300)
    @requires_full_stack(ethminer=True, parity=True, push_client=True)
    async def test_tx_queue_error_propagation(self, *, ethminer, parity, push_client):
        """Tests that a long chain of txs depending on a single transaction propagate errors correctly"""

        default_fees = DEFAULT_STARTGAS * DEFAULT_GASPRICE

        val = 1000 ** 18
        txs = []
        # send funds to address1
        f_tx_hash = await self.send_tx(FAUCET_PRIVATE_KEY, TEST_ADDRESS_1, val)

        await self.ensure_confirmed(f_tx_hash)

        # make sure no blocks are mined
        ethminer.pause()

        addresses = [(TEST_ADDRESS_1, TEST_PRIVATE_KEY_1),
                     (TEST_ADDRESS_2, TEST_PRIVATE_KEY_2),
                     (TEST_ADDRESS_3, TEST_PRIVATE_KEY_3),
                     (TEST_ADDRESS_4, TEST_PRIVATE_KEY_4),
                     (TEST_ADDRESS_5, TEST_PRIVATE_KEY_5),
                     (TEST_ADDRESS_6, TEST_PRIVATE_KEY_6)]

        async with self.pool.acquire() as con:
            for addr, pk in addresses:
                await con.fetch("INSERT INTO notification_registrations (service, registration_id, toshi_id, eth_address) VALUES ($1, $2, $3, $4)",
                                'gcm', "abc", addr, addr)

        for i in range(len(addresses) * 2):
            val = val - default_fees
            addr1, pk1 = addresses[0]
            addr2, pk2 = addresses[1]
            # send funds
            tx_hash = await self.send_tx(pk1, addr2, val)
            txs.append(tx_hash)
            # swap all the variables
            addresses = addresses[1:] + [addresses[0]]
            await push_client.get()
            await push_client.get()

        # get the nonce of the first transaction
        async with self.pool.acquire() as con:
            nonce = await con.fetchval("SELECT nonce FROM transactions WHERE hash = $1", txs[0])

        # overwrite address 1's first tx
        tx = await self.get_tx_skel(TEST_PRIVATE_KEY_1, FAUCET_ADDRESS, val, nonce=nonce)
        tx = sign_transaction(tx, TEST_PRIVATE_KEY_1)
        rpcclient = JsonRPCClient(parity.dsn()['url'])
        await rpcclient.eth_sendRawTransaction(tx)

        await self.ensure_errors(*txs)

        # make sure we got error pns for all
        for i in range(len(addresses) * 2):
            await push_client.get()
            await push_client.get()
        # and the pn for the overwritten tx
        await push_client.get()

    @gen_test(timeout=300)
    @requires_full_stack(ethminer=True, parity=True)
    async def test_unable_to_overspend(self, *, ethminer, parity):
        """Tests that a tx with pending funds cannot overspend them"""

        # make sure no blocks are mined
        ethminer.pause()

        val = 10 ** 18
        val2 = 10 ** 18 - 10 ** 17

        # send funds to address1
        await self.send_tx(FAUCET_PRIVATE_KEY, TEST_ADDRESS_1, val)

        # send most of the user's funds
        await self.send_tx(TEST_PRIVATE_KEY_1, TEST_ADDRESS_2, val2)
        # try send them again
        tx2 = await self.get_tx_skel(TEST_PRIVATE_KEY_1, TEST_ADDRESS_3, val2)
        await self.sign_and_send_tx(TEST_PRIVATE_KEY_1, tx2, expected_response_code=400)

        # get the nonce of the first transaction
        async with self.pool.acquire() as con:
            count = await con.fetchval("SELECT COUNT(*) FROM transactions")
        self.assertEqual(count, 2)

    @gen_test(timeout=60)
    @requires_full_stack(ethminer=True)
    async def test_adding_to_queue_slowly(self, *, ethminer):
        """There was an issue with spamming transactions from a client and the tx queue
        finding complete txs when it shouldn't due to race conditions"""

        val1 = 10 ** 18
        txs = []
        for _ in range(0, 10):
            # send funds to address1
            tx_hash = await self.send_tx(FAUCET_PRIVATE_KEY, TEST_ADDRESS_1, val1)
            txs.append(tx_hash)
            # wait a bit between transactions
            await asyncio.sleep(2)

        await self.ensure_confirmed(*txs)

    @gen_test(timeout=300)
    @requires_full_stack(ethminer=True)
    async def test_tx_queue_ping_pong_with_live_miner(self, *, ethminer):
        """Tests that funds can be ping ponged between accounts
        tests for cases where balance would get messed up due to race
        conditions in block processing and getting the current balance
        """

        default_fees = DEFAULT_STARTGAS * DEFAULT_GASPRICE

        val = 1000 ** 18
        txs = []
        # send funds to address1
        tx_hash = await self.send_tx(FAUCET_PRIVATE_KEY, TEST_ADDRESS_1, val)
        txs.append(tx_hash)

        addr1, pk1 = TEST_ADDRESS_1, TEST_PRIVATE_KEY_1
        addr2, pk2 = TEST_ADDRESS_2, TEST_PRIVATE_KEY_2
        for i in range(10):
            val = val - default_fees
            # send funds
            tx_hash = await self.send_tx(pk1, addr2, val)
            txs.append(tx_hash)
            # swap all the variables
            addr1, pk1, addr2, pk2 = addr2, pk2, addr1, pk1
            # 3 was the magic number that when this test was failing it would
            # always fail
            await asyncio.sleep(3)

        await self.ensure_confirmed(*txs)

class LargeVolumeTest(FaucetMixin, EthServiceBaseTest):

    @gen_test(timeout=300)
    @requires_full_stack
    async def test_large_volume_of_txs(self):
        """Tests that the service can handle a large volume of transactions
        from different users
        """

        num_of_txs = 10
        # create sending user keys
        from_keys = [os.urandom(32) for _ in range(num_of_txs)]
        # create receiver user addresses
        to_addresses = [data_encoder(os.urandom(20)) for _ in range(num_of_txs)]
        # faucet sending users
        txs = []
        for key in from_keys:
            addr = private_key_to_address(key)
            txs.append((await self.send_tx(FAUCET_PRIVATE_KEY, addr, 10 * (10 ** 18))))
        # send transactions
        import concurrent.futures
        loop = asyncio.get_event_loop()
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            futures = [loop.run_in_executor(executor, self.send_tx, key, address, 10 ** 18) for key, address in zip(from_keys, to_addresses)]
            for future in concurrent.futures.as_completed(futures):
                tx = future.result()
                txs.append(tx)
        # wait for completion of all
        # async with self.pool.acquire() as con:
        #     rows = await con.fetch("SELECT * FROM transactions")
        # for row in rows:
        #     if row['hash'] not in txs:
        #         print("NOT IN TXS")
        #     print(row)
        while True:
            async with self.pool.acquire() as con:
                rows = await con.fetch("SELECT status, COUNT(*) FROM transactions WHERE hash = ANY($1) GROUP BY status",
                                       txs)
            s = []
            for row in rows:
                s.append("{}: {}".format(row['status'], row['count']))
                if row['status'] == 'confirmed' and row['count'] == len(txs):
                    return
                if row['status'] == 'queued' and row['count'] == 1:
                    async with self.pool.acquire() as con:
                        row = await con.fetchrow("SELECT * FROM transactions WHERE status = 'queued'")
                    print(row)

            print(', '.join(s))
            await asyncio.sleep(1)
