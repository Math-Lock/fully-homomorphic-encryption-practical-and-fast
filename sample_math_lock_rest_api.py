import requests
import json
import time


class SampleMathLockApp:
    """
    TODO: Please carefully read below description - it's not mandatory for the usage of sample, but very important
    TODO: for general understanding.
    Sample app to be able to test Math-Lock solution for Fully Homomorphic Encryption (FHE)
    It provides capabilities based on REST API to perform unique, simple and fast operations, such as:
        - Fully Homomorphic Decryption
        - Fully Homomorphic Encryption,
        - Fully Homomorphic arithmetic ops over the ciphertext: Division, Subtraction, Addition and Multiplication

    The reason of giving it via REST API is very simple:
        1) It's very easy to use and also very easy to implement for a demo purpose
        2) we are not going to open our code/algorithms, because it's our 'know-how' and many years of development

    Our FHE scheme is capable to do all the things without Noise and its reduction, bootstrapping, etc crap, which all
    existing FHE schemes in the World are suffering with.

    Entire algorithms are based on matrices 2x2 (for Demo) so all ciphertexts you will see as 2x2 matrices
    Security is based on the system of complex non-linear equations, and key length computed from minimum 64 bit length
    number for each matrix cell. Security key is hardcoded on the server side and we not gonna share it so far.
    Also important to note, that we can do both symmetric and asymmetric algorithms, both integers and floating-point.

    ####### ----- #######

    PERFORMANCE BENCHMARKS based on Linux Ubuntu 20.04 LTE, Dell laptop, with i7-11850H on board
    all performance results can be found here: https://www.math-lock.com/benchmarks.html

    Take a note that testing it via REST API of course will give huge performance reduction/degradation
    According to our measurements, REST version is limited for every operation type up to 1000 ops / sec (local host)
    and around 10 ops / sec using our web-site. Due to many restrictions, such as data conversion overhead, network,
    Flask etc things.
    Precision - in REST version it's limited to 6 digits, only for decrypt operations - just to simplify an answer
    """
    def __init__(self) -> None:
        self.base_url = 'https://math-lock.com'
        self.port = 443
        self.api_suffix = r"/api"
        self.encrypt = r"/encrypt"
        self.decrypt = r"/decrypt"
        self.math = r"/math"
        self.headers = {"Content-Type": "application/json", "host": "www.math-lock.com"}

    def run_test(self, num1: str, num2: str) -> None:
        """ The method launches all available operations to test functionality """
        print(f"Num 1 to operate with is: {num1}, Num2: {num2}")
        print("Let's check ourselves! :)")
        print(f"Num1 multiplication Num2 in a standard way: {float(num1) * float(num2)}")
        print(f"Num1 addition Num2 in a standard way: {float(num1) + float(num2)}")
        print(f"Num1 division Num2 in a standard way: {float(num1) / float(num2)}")
        print(f"Num1 subtraction Num2 in a standard way: {float(num1) - float(num2)}")

        print("\nNow, let's encrypt our data and perform math. homomorphic operations over the ciphertext'\n")
        encrypted1 = self.do_encryption(num1)
        encrypted2 = self.do_encryption(num2)

        res_mult = self.do_multiplication(encrypted1, encrypted2)
        print(f"Ciphertext for multiplication of Num1 and Num2: {res_mult}")
        dec_mult = self.do_decryption(res_mult)
        print(f"Decryption of multiplication of Num1 and Num2: {dec_mult['value']}")

        res_add = self.do_addition(encrypted1, encrypted2)
        print(f"Ciphertext for addition of Num1 and Num2: {res_add}")
        dec_add = self.do_decryption(res_add)
        print(f"Decryption of addition of Num1 and Num2: {dec_add['value']}")

        res_div = self.do_division(encrypted1, encrypted2)
        print(f"Ciphertext for division of Num1 and Num2: {res_div}")
        dec_div = self.do_decryption(res_div)
        print(f"Decryption of division of Num1 and Num2: {dec_div['value']}")

        res_subtr = self.do_subtraction(encrypted1, encrypted2)
        print(f"Ciphertext for subtraction of Num1 and Num2: {res_subtr}")
        dec_subtr = self.do_decryption(res_subtr)
        print(f"Decryption of subtraction of Num1 and Num2: {dec_subtr['value']}")

    def run_rest_perf_test(self, num1: str, num2: str, counter: int = 10) -> None:
        """ The method launches simple perf test """
        print("\nRunning simple perf test to check REST API performance. Executing 10 POST requests for multiplication,"
              "\nDon't forget - REST Api performance has nothing to do with the real performance of our scheme, "
              "it's just a demo with all the restrictions to Net bandwidth etc delays. \nIt's very easy to check - "
              "just use arbitrary huge numbers to operate with and you will see no difference for the speed")

        encrypted1 = self.do_encryption(num1)
        encrypted2 = self.do_encryption(num2)

        start_time = time.perf_counter()
        my_range = range(0, counter)
        for _ in my_range:
            self.do_multiplication(encrypted1, encrypted2)

        time_finish = '{:.5f}'.format(time.perf_counter() - start_time)
        print(f"Time took for {counter} iterations of multiplication is: {time_finish} seconds")

    def prepare_math(self, m1: dict, m2: dict, ops: str) -> json:
        """ builds basic json for arithmetic operations """
        return {"num1": m1, "num2": m2, "ops_type": ops}

    def do_encryption(self, value: str) -> json:
        """ executes post request to do encryption operation via REST API """
        res = self.__post_encryption({"value": value})
        result = json.loads(res.content.decode())
        del result["error"]

        return result

    def do_decryption(self, m1: dict) -> json:
        """ executes post request to do decryption operation via REST API """
        res = self.__post_decryption(m1)
        result = json.loads(res.content.decode())
        del result["error"]

        return result

    def do_subtraction(self, m1: dict,  m2: dict) -> json:
        """ executes post request to do subtraction operation over the ciphertext via REST API """
        res = self.__post_math_ops(self.prepare_math(m1, m2, "subtraction"))
        result = json.loads(res.content.decode())
        del result["error"]

        return result

    def do_addition(self, m1: dict, m2: dict) -> json:
        """ executes post request to do addition operation over the ciphertext via REST API """
        res = self.__post_math_ops(self.prepare_math(m1, m2, "addition"))
        result = json.loads(res.content.decode())
        del result["error"]

        return result

    def do_division(self, m1: dict, m2: dict) -> json:
        """ executes post request to do division operation over the ciphertext via REST API """
        res = self.__post_math_ops(self.prepare_math(m1, m2, "division"))
        result = json.loads(res.content.decode())
        del result["error"]

        return result

    def do_multiplication(self, m1: dict, m2: dict) -> json:
        """ executes post request to do multiplication operation over the ciphertext via REST API """
        res = self.__post_math_ops(self.prepare_math(m1, m2, "multiplication"))
        result = json.loads(res.content.decode())
        del result["error"]

        return result

    def __post_encryption(self, data: json) -> requests.models.Response:
        """ builds path and executes post request itself to encrypt data """
        url = f'{self.base_url}:{self.port}{self.api_suffix}{self.encrypt}'
        return requests.post(url, json=data, verify=True, headers=self.headers)

    def __post_decryption(self, data: json) -> requests.models.Response:
        """ builds path and executes post request itself to decrypt data """
        url = f'{self.base_url}:{self.port}{self.api_suffix}{self.decrypt}'
        return requests.post(url, json=data, verify=True, headers=self.headers)

    def __post_math_ops(self, data: json) -> requests.models.Response:
        """ builds path and executes post request itself for all 4 arithmetic ops """
        url = f'{self.base_url}:{self.port}{self.api_suffix}{self.math}'
        return requests.post(url, json=data, verify=True, headers=self.headers)


if __name__ == '__main__':

    # choose any (literally) 2 numbers which you want to operate with.
    # they shall be declared as a string type
    num1_ = "15.5"
    num2_ = "-89.56544"
    my_rest = SampleMathLockApp()
    my_rest.run_test(num1_, num2_)
    my_rest.run_rest_perf_test(num1_, num2_)
