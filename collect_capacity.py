
import psycopg2
import csv
import logging
from datetime import datetime
from vspherecapacity.vcenter.handle import Vcenter, VcenterList
from vspherecapacity.ucs import Ucsd, UcsList
from pycrypt.credstore import Credential
from pyVmomi import vim
from vspherecapacity.capacity.cluster import ClusterCapacity
from vspherecapacity.capacity import DatabaseAccess, DatabaseObject
from log.setup import LoggerSetup
from args.handle import Args


def write_csv(dict_obj, fpath, append=False):
    if append:
        with open(fpath, mode='a') as csv_file:
            csv_writer = csv.DictWriter(csv_file, list(dict_obj.keys()))
            csv_writer.writerow(dict_obj)
            csv_file.close()
    else:
        with open(fpath, mode='w') as csv_file:
            csv_writer = csv.DictWriter(csv_file, list(dict_obj.keys()))
            if not append:
                csv_writer.writeheader()
            csv_writer.writerow(dict_obj)
            csv_file.close()


if __name__ == '__main__':
    args = Args()
    log_setup = LoggerSetup(yaml_file=args.logger_setup_yml)
    log_setup.set_loglevel(args.log_level)
    log_setup.setup()

    log = logging.getLogger(__name__)

    lc_cred = Credential(username=args.lc_db_user).get_credential()
    psql = psycopg2.connect(host=args.lc_dbserver_name_or_ip,
                            database=args.lc_db_name,
                            user=args.lc_db_user,
                            password=lc_cred.retrieve_password(),
                            )
    psql_cursor = psql.cursor()

    sql_qry_columns = "common_name,serial_number,contract_expiring,end_of_life,product_description"
    sql_qry = """SELECT {} FROM corp
              UNION
              SELECT {} FROM stores
              """.format(sql_qry_columns, sql_qry_columns)
    psql_cursor.execute(sql_qry)
    sql_data = [DatabaseObject(columns=sql_qry_columns.split(','), sql_data=dbo) for dbo in psql_cursor.fetchall()]
    psql.close()
    sql_map = {}
    for dbo in sql_data:
        sql_map.update({
            dbo.common_name.lower(): dbo
        })

    ucs_list = UcsList()
    ucs_list.get_list()
    ucs_handler = []
    ucs_hw_map = {}

    for ucs in ucs_list.ucs_list:
        ucs_login = {
            'ip': ucs.get('name', None)
        }
        cred = Credential('oppucs01').get_credential()
        ucs_login.update({
            'username': cred.username,
            'password': cred.retrieve_password(),
        })
        ucs_handler.append(Ucsd(**ucs_login))

    for ucsm in ucs_handler:
        ucsm.connect()
        if ucsm._is_connected():
            ucs_hw_map.update(ucsm.map_service_profile_to_serial_number())
        ucsm.disconnect()

    cluster_capacity = []
    start_time = datetime.now()
    vcenter_list = VcenterList()
    vcenter_list.get_list()
    # vc_list = vcenter_list.parse_list(env_type='prod')
    vc_list = [v for v in vcenter_list.vcenter_list if v.get('env_type', None).lower() != 'storepoc']

    vc_handler = []
    for vcenter in vc_list:
        vc_handler.append(Vcenter(vcenter.get('name', None)))

    for vc in vc_handler:
        log.debug("Starting vCenter Capacity for {}".format(vc.name))
        vc.connect()
        if vc.si:
            vsp_cap = []
            ds_cap = []
            log.debug('{}\tCollecting all clusters'.format(datetime.now()))
            all_clusters = vc.get_container_view([vim.ClusterComputeResource])
            log.debug('{}\tCollecting all clusters Capacities'.format(datetime.now()))
            cluster_capacity.append([ClusterCapacity(vc.name, cl, hw_map=ucs_hw_map, sql_map=sql_map) for cl in all_clusters])
            log.debug('{}\tComplete Collecting all clusters Capacities'.format(datetime.now()))
            vc.disconnect()
            log.debug("End vCenter Capacity for {}".format(vc.name))

    log.debug('{}\tStartDB Updates'.format(datetime.now()))
    cl_capacity_json = []
    vsp_capacity_json = []
    ha_capacity_json = []
    first_write = True
    for list_obj in cluster_capacity:
        for cl_capacity in list_obj:
            cl_cap_dict = cl_capacity.convert_to_json().copy()
            vsp_capacity_json.append(cl_cap_dict['vsp_capacity'].copy())
            [cap.update({'cluster': cl_cap_dict['name']}) for cap in cl_cap_dict['ha_capacity']]
            ha_capacity_json.append(cl_cap_dict['ha_capacity'].copy())
            cl_cap_dict.pop('vm_sizes')
            cl_cap_dict.pop('vsp_capacity')
            cl_cap_dict.pop('datastores')
            cl_cap_dict.pop('datastore_clusters')
            cl_cap_dict.pop('ha_capacity')
            cl_cap_dict.pop('_ClusterCapacity__dbo')
            cl_capacity_json.append(cl_cap_dict)
            cl_capacity.setup_database_connection(db_host=args.dbserver_name_or_ip,
                                                  db_name=args.db_name,
                                                  db_user=args.db_user)
            cl_capacity.db_update_or_create()
    #         if first_write:
    #             first_write = False
    #             write_csv(cl_cap_dict, '/u01/tmp/cluster_capacity.csv')
    #
    #         else:
    #             write_csv(cl_cap_dict, '/u01/tmp/cluster_capacity.csv', append=True)
    #
    # first_write = True
    # for list_obj in ha_capacity_json:
    #     for cap in list_obj:
    #         if first_write:
    #             first_write = False
    #             write_csv(cap, '/u01/tmp/cluster_ha_capacity.csv')
    #         else:
    #             write_csv(cap, '/u01/tmp/cluster_ha_capacity.csv', append=True)
    #
    # first_write = True
    # for list_obj in vsp_capacity_json:
    #     for cap in list_obj:
    #         if first_write:
    #             first_write = False
    #             write_csv(cap, '/u01/tmp/cluster_vsphere_capacity.csv')
    #         else:
    #             write_csv(cap, '/u01/tmp/cluster_vsphere_capacity.csv', append=True)
    #
    #
    # exit(0)
    log.debug('{}\tStartDB Decomm Updates'.format(datetime.now()))
    dba = DatabaseAccess(host='y0319t11888',
                         db='vsphere_capacity',
                         user='django_app')
    dba.update_decommissions(days_missing_before_decomm=3)
    dba.dispose()
    log.debug('{}\tCompleteDB Decomm Updates'.format(datetime.now()))
    log.debug('{}\tCompleteDB Updates'.format(datetime.now()))

