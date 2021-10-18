import datetime
import json
import os
import time

from multiprocessing import Process, Queue, Lock, Event
from dotenv import load_dotenv
from eth_account import Account
from web3.auto import w3

from sensorhub_master.sensorhub.hub import SensorHub


class EnvironmentManager:
    def __init__(self):
        load_dotenv()

    def getenv_or_raise(self, key):
        value = os.getenv(key)
        if value is None:
            raise Exception(f'{key} variable not found in .env')
        return value


class AccountManager:
    def __init__(self, _private_key, _address):
        self.private_key = _private_key
        self.address = _address

    def sign_transaction(self, transaction):
        signed = Account.sign_transaction(transaction_dict=transaction, private_key=self.private_key)
        return signed.rawTransaction

    def send_signed_tx(self, func, value, timestamp):
        nonce = w3.eth.getTransactionCount(self.address)
        raw_tx = func(value, timestamp).buildTransaction(
            {'nonce': nonce, 'gasPrice': 0, 'gas': 300000, 'from': self.address})
        signed_tx = self.sign_transaction(raw_tx)
        hash_tx = w3.eth.send_raw_transaction(signed_tx)
        w3.eth.wait_for_transaction_receipt(hash_tx.hex())
        return hash_tx.hex()


class ContractManager:
    def __init__(self, account_manager):
        with open('./Contract/SimpleTemp.abi') as fabi:
            with open('./Contract/address.txt') as faddr:
                abi = json.load(fabi)
                contract_address = faddr.read()
                self.contract = w3.eth.contract(address=contract_address, abi=abi)
                self.account_manager = account_manager

    def get_contract(self):
        return self.contract

    def get_measurements_func(self):
        return self.contract.get_function_by_name('getMeasurements')

    def store_measurement_func(self):
        return self.contract.get_function_by_name('storeMeasurement')

    def print_measurements(self):
        x = self.contract.caller.getMeasurements()
        i = 0
        for m in x:
            i += 1
            print(i, ": ", "temp: ", m[0], "time: ", datetime.datetime.fromtimestamp(m[1]))

    def store_measurement(self, value, timestamp):
        store_func = self.store_measurement_func()
        hash_tx = self.account_manager.send_signed_tx(store_func, value, timestamp)
        return hash_tx


env_manager = EnvironmentManager()

if not w3.isConnected():
    raise Exception(f'Cannot connect to web3')

private_key = env_manager.getenv_or_raise('SIGNER_LOCAL_PRIVATE_KEY')
address = env_manager.getenv_or_raise('SIGNER_LOCAL_ADDRESS')
checksum = w3.toChecksumAddress(address)
_account_manager = AccountManager(_private_key=private_key, _address=checksum)
contract_manager = ContractManager(_account_manager)
hub = SensorHub()
safe_print = Lock()
q = Queue()
ended = Event()


def producer():
    n = 0
    while n < 100:
        n += 1
        temp = hub.get_off_board_temperature()
        now = int(time.time())
        with safe_print:
            print('temp: ', temp, 'time: ', datetime.datetime.fromtimestamp(now))
        q.put((temp, now))
        time.sleep(1)


def consumer(q, e):
    while not (q.empty() and e.is_set()):
        _temp, _time = q.get()
        hash_tx = contract_manager.store_measurement(_temp, _time)
        with safe_print:
            print("Measurement stored -> hash: ", hash_tx)


p = Process(target=consumer, args = (q, ended, ))
p.start()
producer()
ended.set()
p.join()
contract_manager.print_measurements()
