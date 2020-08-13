--Email: liqiang3@sugon.com
--Usage: Storage testing by oracle sql insert,select,update,delete.

CREATE OR REPLACE PROCEDURE sugon_oracle_longrun_1 (
   v_sizeg   IN NUMBER := 10,
   v_table   IN VARCHAR2 := 'test_table1',
   v_ts      IN VARCHAR2 := 'ts_data',
   v_run     IN NUMBER := 1)
IS
   v_sql             VARCHAR2 (1000);
   v_yn_ts           NUMBER;
   v_lock            NUMBER;
   v_tablespace      VARCHAR2 (100);
   v_yn_table        NUMBER;
   v_random_number   NUMBER;
   v_random_string   VARCHAR2 (1000);
BEGIN
   --check lock
   SELECT COUNT (*)
     INTO v_lock
     FROM v$locked_object
    WHERE object_id =
             (SELECT object_id
                FROM user_objects
               WHERE     object_name = UPPER (v_table)
                     AND object_type = 'TABLE');

   IF v_lock = 0
   THEN
      --create table
      SELECT COUNT (*)
        INTO v_yn_table
        FROM user_tables
       WHERE table_name = UPPER (v_table);

      IF v_yn_table > 0
      THEN
         EXECUTE IMMEDIATE
            'drop  table ' || v_table || ' cascade  constraints';
      END IF;

      SELECT COUNT (*)
        INTO v_yn_ts
        FROM user_tablespaces
       WHERE tablespace_name = UPPER (v_ts);

      IF v_yn_ts > 0
      THEN
         v_tablespace := 'TABLESPACE ' || v_ts;
      ELSE
         v_tablespace := ' ';
      END IF;

      EXECUTE IMMEDIATE
            'CREATE TABLE '
         || v_table
         || ' (ID NUMBER (21),
   INSERT_TIME DATE DEFAULT SYSDATE,
   RANDOM_NUMBER NUMBER (4),
   RANDOM_STRING   CHAR (20)
   ) '
         || v_tablespace
         || '
   PCTFREE 99
   PCTUSED 1   ';

      --Insert data
      FOR sizeg IN 1 .. v_sizeg * 10
      LOOP
         v_sql :=
               'INSERT /*+ append */ 
            INTO '
            || v_table
            || '
             SELECT LEVEL + ('
            || sizeg
            || ' - 1) * 13000,
                    SYSDATE AS inc_datetime,
         TRUNC (DBMS_RANDOM.VALUE (0, 10000)),
                    DBMS_RANDOM.string (''x'', 8) 
               FROM DUAL
         CONNECT BY LEVEL <= 13000';

         EXECUTE IMMEDIATE v_sql;

         COMMIT;
      END LOOP;
   END IF;

   --select data
   FOR sizeg IN 1 .. v_run * 1000
   LOOP
      v_random_number := TRUNC (DBMS_RANDOM.VALUE (0, 10000));

      v_sql :=
            'SELECT * FROM '
         || v_table
         || ' WHERE RANDOM_NUMBER = '
         || v_random_number;

      EXECUTE IMMEDIATE v_sql;
   END LOOP;

   EXECUTE IMMEDIATE 'alter system flush buffer_cache';

   --update data
   FOR sizeg IN 1 .. v_run * 1000
   LOOP
      v_random_number := TRUNC (DBMS_RANDOM.VALUE (0, 10000));
      v_random_string := DBMS_RANDOM.string ('x', 8);

      v_sql :=
            'UPDATE '
         || v_table
         || ' SET RANDOM_STRING = '''
         || v_random_string
         || ''' WHERE RANDOM_NUMBER = '
         || v_random_number;

      EXECUTE IMMEDIATE v_sql;

      COMMIT;
   END LOOP;

   EXECUTE IMMEDIATE 'alter system flush buffer_cache';

   --delete data
   FOR sizeg IN 1 .. v_run * 1000
   LOOP
      v_random_number := TRUNC (DBMS_RANDOM.VALUE (0, 10000));

      v_sql :=
            'DELETE FROM '
         || v_table
         || ' WHERE RANDOM_NUMBER = '
         || v_random_number;

      EXECUTE IMMEDIATE v_sql;

      COMMIT;
   END LOOP;

   DBMS_LOCK.SLEEP (10);

   --drop table
   SELECT COUNT (*)
     INTO v_lock
     FROM v$locked_object
    WHERE object_id =
             (SELECT object_id
                FROM user_objects
               WHERE     object_name = UPPER (v_table)
                     AND object_type = 'TABLE');

   IF v_lock = 0
   THEN
      EXECUTE IMMEDIATE 'drop  table ' || v_table || ' cascade  constraints';
   END IF;
END;
/
