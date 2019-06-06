
import requests
from urllib3 import disable_warnings
from urllib3.exceptions import InsecureRequestWarning
from pyucs.ucs.handler import Ucs


class UcsList(object):
    def __init__(self, cig_web='cigweb-dev'):
        self.session = requests.Session()
        self.session.verify = False
        disable_warnings(InsecureRequestWarning)
        self.uri = 'http://{}/rest/ucsdomains/'.format(cig_web)
        self.ucs_list = []

    def get_list(self, retrn=False):
        response = self.session.get(self.uri)
        data = response.json()
        self.ucs_list = data['results']
        while data.get('next' or None):
            response = self.session.get(data.get('next'))
            data = response.json()
            self.ucs_list = self.ucs_list.__add__(data['results'])
        self.ucs_list = [ucs for ucs in self.ucs_list if not ucs['decommissioned']]
        if retrn:
            return self.ucs_list

    def parse_list(self, env_type):
        tmp = [u for u in self.ucs_list if u.get('env_prod_test', None).lower() == env_type.lower()]
        return tmp


class Ucsd(Ucs):

    def map_service_profile_to_serial_number(self):

        map_obj = {}
        for sp in self.get_service_profile():
            if sp.assign_state == 'assigned':
                blade = self.query_dn(dn=sp.pn_dn)
                map_obj.update({
                    sp.name.lower(): {
                        'ucsm': self.ucs,
                        'serial': blade.serial,
                        'model': blade.model
                    }
                })

        return map_obj
