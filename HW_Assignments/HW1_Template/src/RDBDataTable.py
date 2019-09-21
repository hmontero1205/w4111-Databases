from src.BaseDataTable import BaseDataTable
import pymysql

class RDBDataTable(BaseDataTable):

    """
    The implementation classes (XXXDataTable) for CSV database, relational, etc. with extend the
    base class and implement the abstract methods.
    """

    def __init__(self, table_name, connect_info, key_columns, commit=None):
        """

        :param table_name: Logical name of the table.
        :param connect_info: Dictionary of parameters necessary to connect to the data.
        :param key_columns: List, in order, of the columns (fields) that comprise the primary key.
        """
        self._data = {
            "table_name": table_name,
            "connect_info": connect_info,
            "key_columns": key_columns,
        }

        self._do_commit = False if commit is None else commit

        self.db_connection = pymysql.connect(host=self._data["connect_info"]["host"],
                                             user=self._data["connect_info"]["user"],
                                             password=self._data["connect_info"]["password"],
                                             db=self._data["connect_info"]["database"],
                                             charset=self._data["connect_info"]["charset"],
                                             cursorclass=pymysql.cursors.DictCursor)




    def find_by_primary_key(self, key_fields, field_list=None):
        """

        :param key_fields: The list with the values for the key_columns, in order, to use to find a record.
        :param field_list: A subset of the fields of the record to return.
        :return: None, or a dictionary containing the requested fields for the record identified
            by the key.
        """
        if not self._data["key_columns"]:
            raise Exception("no primary key defined")

        if len(key_fields) != len(self._data["key_columns"]):
            raise Exception("expected {} field(s) for primary key, got {}".format(len(self._data["key_columns"]),
                                                                               len(key_fields)))
        template = dict(zip(self._data["key_columns"], key_fields))

        return self.find_by_template(template, field_list)

    def find_by_template(self, template, field_list=None, limit=None, offset=None, order_by=None):
        """

        :param template: A dictionary of the form { "field1" : value1, "field2": value2, ...}
        :param field_list: A list of request fields of the form, ['fielda', 'fieldb', ...]
        :param limit: Do not worry about this for now.
        :param offset: Do not worry about this for now.
        :param order_by: Do not worry about this for now.
        :return: A list containing dictionaries. A dictionary is in the list representing each record
            that matches the template. The dictionary only contains the requested fields.
        """
        curr = self.db_connection.cursor()
        tb_name = self._data["table_name"]
        cols = "*" if not field_list else ",".join(field_list)
        filters = 1 if not template else " AND ".join(
            ["{}='{}'".format(col_name, col_val) for col_name, col_val in template.items()])
        sql_select = "SELECT {} FROM {} WHERE {}".format(cols, tb_name, filters)
        curr.execute(sql_select)

        return curr.fetchall()

    def delete_by_key(self, key_fields):
        """

        Deletes the record that matches the key.

        :param key_fields: List of value for the key fields.
        :return: A count of the rows deleted.
        """
        if not self._data["key_columns"]:
            raise Exception("no primary key defined")

        if len(key_fields) != len(self._data["key_columns"]):
            raise Exception("expected {} field(s) for primary key, got {}".format(len(self._data["key_columns"]),
                                                                                  len(key_fields)))

        template = dict(zip(self._data["key_columns"], key_fields))

        return self.delete_by_template(template)

    def delete_by_template(self, template):
        """

        :param template: Template to determine rows to delete.
        :return: Number of rows deleted.
        """
        curr = self.db_connection.cursor()
        tb_name = self._data["table_name"]
        filters = 1 if not template else " AND ".join(
            ["{}='{}'".format(col_name, col_val) for col_name, col_val in template.items()])
        sql_delete = "DELETE FROM {} WHERE {}".format(tb_name, filters)
        num_deleted = curr.execute(sql_delete)

        if self._do_commit:
            self.db_connection.commit()
        return num_deleted

    def update_by_key(self, key_fields, new_values):
        """

        :param key_fields: List of value for the key fields.
        :param new_values: A dict of field:value to set for updated row.
        :return: Number of rows updated.
        """
        if not self._data["key_columns"]:
            raise Exception("no primary key defined")

        if len(key_fields) != len(self._data["key_columns"]):
            raise Exception("expected {} field(s) for primary key, got {}".format(len(self._data["key_columns"]),
                                                                               len(key_fields)))

        template = dict(zip(self._data["key_columns"], key_fields))

        return self.update_by_template(template, new_values)

    def update_by_template(self, template, new_values):
        """

        :param template: Template for rows to match.
        :param new_values: New values to set for matching fields.
        :return: Number of rows updated.
        """
        curr = self.db_connection.cursor()
        tb_name = self._data["table_name"]
        filters = 1 if not template else " AND ".join(
            ["{}='{}'".format(col_name, col_val) for col_name, col_val in template.items()])
        updates = ",".join(["{}='{}'".format(k,v) for k,v in new_values.items()])
        sql_update = "UPDATE {} SET {} WHERE {}".format(tb_name, updates, filters)
        num_updated = curr.execute(sql_update)

        if self._do_commit:
            self.db_connection.commit()

        return num_updated
    def insert(self, new_record):
        """

        :param new_record: A dictionary representing a row to add to the set of records.
        :return: None
        """
        curr = self.db_connection.cursor()
        tb_name = self._data["table_name"]
        col_names = ",".join(new_record.keys())
        vals = ",".join(["'{}'".format(v) for v in new_record.values()])
        sql_insert = "INSERT INTO {} ({}) VALUES ({})".format(tb_name, col_names, vals)
        curr.execute(sql_insert)
        if self._do_commit:
            self.db_connection.commit()

    def get_rows(self):
        return self.find_by_template(None)




