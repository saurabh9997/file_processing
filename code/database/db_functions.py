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

    def upload_file(self, filelocation):
        """
        Uploading large CSV file
        :param filelocation: location of processed file
        :return: error if present
        """
        try:
            con = self.__check_connection()
            cur = con.cursor()
            query = f"""
                        begin;
    
                        create schema if not exists product;
    
                        create table product.products
                                                (
                                                    name varchar, sku varchar primary key, description varchar
                                                );
    
                        copy product.products FROM '{filelocation}'
                        WITH (format csv, header) ;
                        commit;
                    """
            cur.execute(query)
            con.commit()
        except (Exception, psycopg2.Error) as error:
            return "Error while bulk upload data from PostgreSQL", error
        finally:
            # closing database connection.
            if con:
                cur.close()
                con.close()

    def update_table(self, filelocation):
        """
        Function is used update table in database
        :param filelocation: filelocation where data is stored
        """
        try:
            con = self.__check_connection()
            cur = con.cursor()
            query = f"""
                            begin;
    
                            create temp table batch
                                                    (
                                                        like product.products
                                                        including all
                                                    )
                            on commit drop;
    
                            copy batch  FROM '{filelocation}'
                            WITH (format csv, header);
    
                            with upd as
                                        (
                                            update product.products
                                                set (name, sku, description)
                                                    = (batch.name, batch.sku, batch.description)
    
                                            from batch
    
                                            where batch.sku = product.products.sku and (product.products.name, product.products.sku, product.products.description) <> (batch.name, batch.sku, batch.description)
    
                                            returning product.products.sku
                                        ),
                                        ins as
                                        (
                                            insert into product.products
                                                select name, sku, description
                                                        from batch
                                                 where not exists
                                        (
                                                select 1
                                                from product.products
                                                where test.table.sku = batch.sku
                                        )
                                        returning product.products.sku
                                        )
                            commit;
                            """
            cur.execute(query)
            con.commit()
        except (Exception, psycopg2.Error) as error:
            return "Error while updating data from PostgreSQL", error
        finally:
            # closing database connection.
            if con:
                cur.close()
                con.close()

    def fetch_table_data(self, limit):
        """
        Function is used to fetch data from database in descending order
        :param limit: no of data we want
        :return: list of data dictionary
        """
        try:
            con = self.__check_connection()
            cur = con.cursor()
            data_limit = limit
            data_list = []
            query = """select name, count(*) from product.products group by name ORDER BY 2 DESC;"""
            cur.execute(query)
            if data_limit:
                data = cur.fetchmany(data_limit)
                for i in data:
                    data_list.append({"name": i[0], "no. of products": i[1]})
            else:
                data = cur.fetchall()
                for i in data:
                    data_list.append({"name": i[0], "no. of products": i[1]})
            con.commit()
            return data_list
        except (Exception, psycopg2.Error) as error:
            return "Error while fetching data from PostgreSQL", error
        finally:
            # closing database connection.
            if con:
                cur.close()
                con.close()


if __name__ == "__main__":
    d = DBConnection("postman", "postgres", "S@jha1234", "localhost")
    print(d.upload_file("a", "b", "c"))
