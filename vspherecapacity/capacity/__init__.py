
import csv
import psycopg2
import time
import random
from datetime import datetime, timedelta
from vspherecapacity.credentials.credstore import Credential
from log.setup import addClassLogger


@addClassLogger
class CapacitySuper(object):

    def write_csv(self, fpath):
        dict_obj = self.convert_to_json()
        with open(fpath, mode='w') as csv_file:
            csv_writer = csv.DictWriter(csv_file, list(dict_obj.keys()))
            csv_writer.writeheader()
            csv_writer.writerow(dict_obj)

            csv_file.close()

    def convert_to_json(self):
        dict_obj = self.__dict__
        for key in list(dict_obj.keys()):
            if isinstance(dict_obj[key], list):
                for i in dict_obj[key]:
                    index = dict_obj[key].index(i)
                    dict_obj[key][index] = i.convert_to_json()
        return dict_obj


@addClassLogger
class DatabaseObject(object):

    def __init__(self, columns, sql_data):
        for column in columns:
            setattr(self, column.strip(), sql_data[columns.index(column)])


@addClassLogger
class DatabaseAccess(object):

    def __init__(self, host, db, user, password=None):
        self.host = host
        self.database = db
        self.credential = Credential(username=user, password=password)
        self.connection = psycopg2.connect(host=self.host,
                                           database=self.database,
                                           user=self.credential.username,
                                           password=self.credential.retrieve_password(),
                                           )
        self.cursor = self.connection.cursor()

    def initialize_cursor(self):
        self.cursor.close()
        self.cursor = self.connection.cursor()

    def dispose(self):
        self.cursor.close()
        self.connection.close()

    def get_dbo(self, model, obj, select_columns='*', where_param='_mo_id'):
        if not select_columns == '*':
            sql_qry = "SELECT id,{} FROM {} WHERE {}.{}=%s ;".format(select_columns, model, model, where_param)
        else:
            sql_qry = "SELECT {} FROM {} WHERE {}.{}=%s ;".format(select_columns, model, model, where_param)
        self.cursor.execute(sql_qry, (obj[where_param],))
        return self.cursor.fetchall()

    def update_or_create_dbo(self, obj, model, columns=None, where_param='_mo_id', skip_date=False):
        values = tuple(obj.values())
        value_interop = ('%s,' * len(values)).rstrip(',')

        if not columns:
            columns = tuple(obj.keys())
            if len(columns) == 1:
                column_interop = columns[0]
            elif len(columns) > 1:
                column_interop = ', '.join(columns)

        if columns and not len(columns) == len(values):
            raise ValueError(
                "Invalid value for parameter 'columns'. Number of items in columns must match values"
            )

        existing_dbo = self.get_dbo(model=model,
                                    obj=obj,
                                    where_param=where_param)

        sql_qry = ''
        if existing_dbo:
            if not skip_date:
                column_interop += ", date_modified, decommission, decommission_date"
                value_interop += ", %s, %s, %s"
                obj.update({
                    'date_modified': datetime.now(),  # Todo: This is production: uncomment, remove next line
                    # 'date_modified': datetime.now() - timedelta(days=random.randint(0, 10), ),  # Todo: dev code only
                    'decommission': False,
                    'decommission_date': None})
            values = list(obj.values())
            values.append(obj[where_param])
            sql_qry = "UPDATE {} SET ({}) = ({}) WHERE {}.{}=%s;".format(model,
                                                                         column_interop,
                                                                         value_interop,
                                                                         model,
                                                                         where_param
                                                                         )
        elif not existing_dbo:
            if not skip_date:
                column_interop += ", date_created, date_modified, decommission"
                value_interop += ", %s, %s, %s"
                obj.update({
                    'date_created': datetime.now(),
                    'date_modified': datetime.now(),  # Todo: This is production uncomment, remove next line
                    # 'date_modified': datetime.now() - timedelta(days=random.randint(0, 10),),  # Todo: dev code only
                    'decommission': False
                })
            values = tuple(obj.values())
            sql_qry = "INSERT INTO {} ({}) VALUES({}) ;".format(model,
                                                                column_interop,
                                                                value_interop)
        self.cursor.execute(sql_qry, tuple(values))
        self.connection.commit()

    def decommission_dbo(self, obj, model, where_param='id'):
        values = tuple(obj.values())
        value_interop = ('%s,' * len(values)).rstrip(',')

        columns = tuple(obj.keys())
        column_interop = ', '.join(columns)

        if columns and not len(columns) == len(values):
            raise ValueError(
                "Invalid value for parameter 'columns'. Number of items in columns must match values"
            )
        values = list(values)
        values.append(obj[where_param])
        values = tuple(values)

        sql_qry = "UPDATE {} SET ({}) = ({}) WHERE {}.{}=%s;".format(model,
                                                                     column_interop,
                                                                     value_interop,
                                                                     model,
                                                                     where_param
                                                                     )

        self.cursor.execute(sql_qry, tuple(values))
        self.connection.commit()

    def update_decommissions(self, days_missing_before_decomm=3):
        decom_point = datetime.now()
        sql_qry_tables = "SELECT table_name from information_schema.tables where table_name LIKE 'capacity_%' ;"
        self.cursor.execute(sql_qry_tables)
        tables_dbo = self.cursor.fetchall()

        decom_dbo_map = {}
        for index in tables_dbo:
            table = index[0]
            if not decom_dbo_map.get(table or None):
                decom_dbo_map.update({table: []})

            sql_qry = "SELECT * FROM {} WHERE {}.date_modified < NOW() - INTERVAL '{} days' ;".format(table,
                                                                                                      table,
                                                                                                      days_missing_before_decomm)
            dbo = None
            try:
                self.cursor.execute(sql_qry)
                dbo = self.cursor.fetchall()
            except psycopg2.errors.UndefinedColumn as e:
                self.connection.rollback()
                pass
            except BaseException:
                self.connection.rollback()
                raise

            if dbo:
                decom_dbo_map[table] = dbo

        for table in list(decom_dbo_map.keys()):
            for dbo in decom_dbo_map[table]:
                obj = {'id': dbo[00],
                       'decommission': True,
                       'decommission_date': decom_point
                       }
                self.decommission_dbo(obj=obj,
                                      model=table,
                                      where_param='id')
                fkey_map = self.map_foreign_keys(table)
                if fkey_map:
                    for f_obj in fkey_map[table]:
                        self.remove_decommissioned_relationships(model=f_obj.table,
                                                                 foreign_key={f_obj.key: dbo[00]},
                                                                 where_param=f_obj.key)

    def map_foreign_keys(self, model):
        sql_qry = """SELECT
            tc.table_name,
            kcu.column_name,
            ccu.table_name AS foreign_table_name
        FROM
            information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY' and ccu.table_name=%s;"""
        self.cursor.execute(sql_qry, (model,))
        dbo = self.cursor.fetchall()

        if dbo:
            foreign_map = {}
            for i in dbo:
                table = i[0]
                column = i[1]
                f_table = i[2]

                if not foreign_map.get(f_table or None):
                    foreign_map.update({f_table: []})
                foreign_map[f_table].append(ForeignKey(table, f_table, column))

            return foreign_map
        return None

    def remove_decommissioned_relationships(self, model, foreign_key, where_param):
        dbo = self.get_dbo(model=model,
                           obj=foreign_key,
                           where_param=where_param)
        if dbo:
            sql_qry = """DELETE FROM {} WHERE {}.{}=%s ;""".format(model,
                                                                   model,
                                                                   where_param)
            self.cursor.execute(sql_qry, (foreign_key[where_param],))
            self.connection.commit()

    def remove_dbo(self, model, obj, where_param):
        dbo = self.get_dbo(model=model,
                           obj=obj,
                           where_param=where_param)
        if dbo:
            sql_qry = """DELETE FROM {} WHERE {}.{}=%s ;""".format(model,
                                                                   model,
                                                                   where_param)
            self.cursor.execute(sql_qry, (obj[where_param],))
            self.connection.commit()


@addClassLogger
class ForeignKey:

    def __init__(self, table, foreign_table, key):
        self.table = table
        self.foreign_table = foreign_table
        self.key = key
