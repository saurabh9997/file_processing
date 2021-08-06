import psycopg2


class DBConnection:
    def __init__(self, dbname, user, password, host):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host

    def __check_connection(self):
        """
        This class is private because this variable can not be called outside the class
        :return: connection cursor
        """
        conn = psycopg2.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            dbname=self.dbname
        )
        if conn:
            return conn

    def bulk_upload_file(self, filelocation):
        con = self.__check_connection()
        cur = con.cursor()
        query = f"""
                    begin;

                    create schema if not exists test;

                    create table test.table
                                            (
                                                name varchar, sku varchar primary key, description varchar
                                            );

                    copy test.table FROM '{filelocation}'
                    WITH (format csv, header) ;
                    commit;
                """
        cur.execute(query)
        con.commit()
        con.close()

    def update_table(self, filelocation):
        con = self.__check_connection()
        cur = con.cursor()
        query = f"""
                        begin;

                        create temp table batch
                                                (
                                                    like test.table
                                                    including all
                                                )
                        on commit drop;

                        copy batch  FROM '{filelocation}'
                        WITH (format csv, header);

                        with upd as
                                    (
                                        update test.table
                                            set (name, sku, description)
                                                = (batch.name, batch.sku, batch.description)

                                        from batch

                                        where batch.sku = test.table.sku and (test.table.name, test.table.sku, test.table.description) <> (batch.name, batch.sku, batch.description)

                                        returning test.table.sku
                                    ),
                                    ins as
                                    (
                                        insert into test.table
                                            select name, sku, description
                                                    from batch
                                             where not exists
                                    (
                                            select 1
                                            from test.table
                                            where test.table.sku = batch.sku
                                    )
                                    returning test.table.sku
                                    )
                        select (select count(*) from upd) as updates,
                                (select count(*) from ins) as inserts;

                        commit;
                        """
        cur.execute(query)
        con.commit()
        con.close()


if __name__ == "__main__":
    d = DBConnection("postman", "postgres", "S@jha1234", "localhost")
    print(d.insert_data("a", "b", "c"))
