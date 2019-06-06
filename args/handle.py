
import argparse
from configparser import ConfigParser
from configparser import NoOptionError
from pycrypt.encryption import AESCipher


class Args:
    """
    Args Class handles the cmdline arguments passed to the code and
    parses through a conf file
    Usage can be stored to a variable or called by Args().<property>
    """

    def __init__(self):
        self.__aes_key = None

        # Retrieve and set script arguments for use throughout
        parser = argparse.ArgumentParser(description="Deploy a new VM Performance Collector VM.")
        parser.add_argument('-debug', '--debug',
                            required=False, action='store_true',
                            help='Used for Debug level information')
        parser.add_argument('-c', '--config-file', default='/etc/metrics/metrics.conf',
                            required=False, action='store',
                            help='identifies location of the config file')
        cmd_args = parser.parse_args()

        self.DEBUG = cmd_args.debug

        # Parse through the provided conf
        parser = ConfigParser()
        parser.read(cmd_args.config_file)

        # [LOGGING]
        self.logger_setup_yml = str(parser.get('logging', 'LoggerSetupPath'))
        self.LOG_SIZE = parser.get('logging', 'LogRotateSizeMB')
        self.MAX_KEEP = parser.get('logging', 'MaxFilesKeep')

        try:
            debug_check = parser.get('logging', 'Debug')
            if debug_check.lower() == 'true':
                self.DEBUG = True
        except NoOptionError:
            pass

        if self.DEBUG:
            self.log_level = 'DEBUG'
        else:
            self.log_level = 'INFO'

        # [PSQLDB]
        self.dbserver_name_or_ip = parser.get('psqldb', 'NameOrIp')
        self.db_name = parser.get('psqldb', 'DatabaseName')
        self.port = parser.get('psqldb', 'port')
        self.db_user = parser.get('psqldb', 'user')
        self.__password = parser.get('psqldb', 'password')
        if self.__password:
            self.store_passwd(self.__password)

        # [LIFECYCLEDB]
        self.lc_dbserver_name_or_ip = parser.get('lifecycledb', 'NameOrIp')
        self.lc_db_name = parser.get('lifecycledb', 'DatabaseName')
        self.lc_port = parser.get('lifecycledb', 'port')
        self.lc_db_user = parser.get('lifecycledb', 'user')
        self.__lc_password = parser.get('lifecycledb', 'password')
        if self.__lc_password:
            self.store_lc_passwd(self.__lc_password)

        # [UCS]
        self.ucs_user = parser.get('ucs', 'user')

    def get_passwd(self):
        """
        Returns the stored encrypted password from memory
        :return: clear_text password
        """
        if self.__password:
            aes_cipher = AESCipher()
            return aes_cipher.decrypt(self.__password, self.__aes_key)

    def store_passwd(self, clr_passwd):
        """
        Takes the clear text password and stores it in a variable with AES encryption.
        :param clr_passwd:
        :return: None, stores the password in the protected __ variable
        """
        aes_cipher = AESCipher()
        self.__aes_key = aes_cipher.AES_KEY
        self.__password = aes_cipher.encrypt(clr_passwd)

    def get_lc_passwd(self):
        """
        Returns the stored encrypted password from memory
        :return: clear_text password
        """
        if self.__lc_password:
            aes_cipher = AESCipher()
            return aes_cipher.decrypt(self.__lc_password, self.__aes_key)

    def store_lc_passwd(self, clr_passwd):
        """
        Takes the clear text password and stores it in a variable with AES encryption.
        :param clr_passwd:
        :return: None, stores the password in the protected __ variable
        """
        aes_cipher = AESCipher()
        self.__aes_key = aes_cipher.AES_KEY
        self.__lc_password = aes_cipher.encrypt(clr_passwd)
