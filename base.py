# -*- coding: utf-8 -*-

'''
Created on 1 Septembre 2013
 
@author: jyp

OpenEdge backend for Django.
South Apdaptation done 20131102

Extents handling:
-----------------

When this backend is used on 4GL Tables, the fields with extents are seen like char fields where
each extent value is separated from each other with the ";" character.
If the field contain a ";" char the ";" separator is escaped with the "~" char.
Example :
    Field extents value in OpenEdge are : ["AAA;BBB","CCC"], 
    The driver returns these values : "AAA~;BBB;CCC"
    If the field extents value in OpenEdge are : ["AAA~;~BBB","CCC"],
    The driver returns these values : "AAA~~~;~BBB;CCC"

For writing extents, the values must be provided in the same way than the read operation.
Example:
    If a table have an extent column defined like this : CHAR "x(8)" EXTENT 4, the value must be
    provided like this : "AAA;BBB;CCC;DDD".

'''

try:
    import pyodbc as Database
except ImportError as e:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured("Error loading pyodbc module: %s" % e)

import re


m = re.match(r'(\d+)\.(\d+)\.(\d+)(?:-beta(\d+))?', Database.version)
vlist = list(m.groups())
if vlist[3] is None: vlist[3] = '9999'
pyodbc_ver = tuple(map(int, vlist))
if pyodbc_ver < (2, 0, 38, 9999):
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured("pyodbc 2.0.38 or newer is required; you have %s" % Database.version)

#from django.db.backends import BaseDatabaseWrapper, BaseDatabaseFeatures, BaseDatabaseValidation

## Adaptation django 10
from django.db.backends.base.validation import BaseDatabaseValidation
from django.db.backends.base.features import BaseDatabaseFeatures
from django.db.backends.base.base import BaseDatabaseWrapper

from django.db.backends.signals import connection_created
from django.db.transaction import TransactionManagementError
from django.utils.functional import cached_property

from django.conf import settings
from django import VERSION as DjangoVersion
if DjangoVersion[:2] == (1,2) :
    from django import get_version
    version_str = get_version()
    if 'SVN' in version_str and int(version_str.split('SVN-')[-1]) < 11952: # django trunk revision 11952 Added multiple database support.
        _DJANGO_VERSION = 11
    else:
        _DJANGO_VERSION = 12
elif DjangoVersion[:2] == (2,1):
    _DJANGO_VERSION = 21

## 20191021 Ajout Django 2.2    
elif DjangoVersion[:2] == (2,2):
    _DJANGO_VERSION = 22

## 20200113 Ajout Django 3.0  
elif DjangoVersion[:2] == (3,0):
    _DJANGO_VERSION = 30

elif DjangoVersion[:2] == (1,1):
    _DJANGO_VERSION = 11
elif DjangoVersion[:2] == (1,0):
    _DJANGO_VERSION = 10
elif DjangoVersion[0] == 1:
    _DJANGO_VERSION = 13


else:
    _DJANGO_VERSION = 9


#===============================================================================
# from constants import MAX_CONSTRAINT_NAME    
# from constants import MAX_INDEX_NAME
# from constants import MAX_TABLE_NAME
# from constants import MAX_SEQNAME
#===============================================================================

# from OpenEdge.pyodbc.operations import DatabaseOperations
# from OpenEdge.pyodbc.client import DatabaseClient
# from OpenEdge.pyodbc.creation import DatabaseCreation
# from OpenEdge.pyodbc.introspection import DatabaseIntrospection

from django.db.backends.OpenEdge.operations import DatabaseOperations
from django.db.backends.OpenEdge.client import DatabaseClient
from django.db.backends.OpenEdge.creation import DatabaseCreation
from django.db.backends.OpenEdge.introspection import DatabaseIntrospection

import os
import warnings

warnings.filterwarnings('error', 'The DATABASE_ODBC.+ is deprecated', DeprecationWarning, __name__, 0)


#===============================================================================
# collation = 'Latin1_General_CI_AS'
#===============================================================================

deprecated = (
    ('DATABASE_ODBC_DRIVER', 'driver'),
    ('DATABASE_ODBC_DSN', 'dsn'),
    ('DATABASE_ODBC_EXTRA_PARAMS', 'extra_params'),
)
for old, new in deprecated:
    if hasattr(settings, old):
        warnings.warn(
            "The %s setting is deprecated, use DATABASE_OPTIONS['%s'] instead." % (old, new),
            DeprecationWarning
        )

DatabaseError = Database.DatabaseError
IntegrityError = Database.IntegrityError

class DatabaseFeatures(BaseDatabaseFeatures):
    uses_custom_query_class = True
    can_use_chunked_reads = False
    can_return_id_from_insert = True
    ###can_return_id_from_insert = False
    #uses_savepoints = True
    has_bulk_insert = True
    ## Opendge limit to 32 char long
    supports_long_model_names = False
    #transaction_state = False
    ### 20131031
    supports_sequence_reset = False

    
    @cached_property
    def supports_transactions(self):
        "Confirm support for transactions"
        try:
            # Make sure to run inside a managed transaction block,
            # otherwise autocommit will cause the confimation to
            # fail.
            self.connection.enter_transaction_management()
            self.connection.managed(True)
            cursor = self.connection.cursor()
            cursor.execute('CREATE TABLE "ROLLBACK_TEST" (X INT)')
            self.connection._commit()
            cursor.execute('INSERT INTO "ROLLBACK_TEST" (X) VALUES (8)')
            self.connection._rollback()
            cursor.execute('SELECT COUNT(X) FROM "ROLLBACK_TEST"')
            count, = cursor.fetchone()
            cursor.execute('DROP TABLE "ROLLBACK_TEST"')
            self.connection._commit()
            self.connection._dirty = False
        finally:
            self.connection.leave_transaction_management()
        return count == 0


class DatabaseWrapper(BaseDatabaseWrapper):
    drv_name = None
    driver_needs_utf8 = True
    MARS_Connection = False
    unicode_results = False
    datefirst = 7
    ## 20140412 Django 1.10
    Database = Database

    ## 20170426 Django 1.11
    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations

    
    # OpenEdge default internal codepage
    oecpinternal = 'iso8859-1'

    operators = {
        #
        # Since '=' is used not only for string comparision there is no way
        # to make it case (in)sensitive. It will simply fallback to the
        # database collation.
        'exact': '= %s',
        'iexact': "= UPPER(%s)",
        'contains': "LIKE %s ESCAPE '\\' ",
        'icontains': "LIKE UPPER(%s) ESCAPE '\\' ",
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': "LIKE %s ESCAPE '\\' ",
        'endswith': "LIKE %s ESCAPE '\\' ",
        'istartswith': "LIKE UPPER(%s) ESCAPE '\\' ",
        'iendswith': "LIKE UPPER(%s) ESCAPE '\\' ",

       
        'regex': 'LIKE %s ',
        'iregex': 'LIKE %s ',

        # TODO: freetext, full-text contains...
    }

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)
        
        #=======================================================================
        # 
        # if 'OPTIONS' in self.settings_dict:
        #     self.MARS_Connection = self.settings_dict['OPTIONS'].get('MARS_Connection', False)
        #     self.datefirst = self.settings_dict['OPTIONS'].get('datefirst', 7)
        #     self.unicode_results = self.settings_dict['OPTIONS'].get('unicode_results', False)
        #=======================================================================

        if _DJANGO_VERSION >= 13:
            self.features = DatabaseFeatures(self)
        else:
            self.features = DatabaseFeatures()
        self.ops = DatabaseOperations(self)
        
        #=======================================================================
        # self.MAX_TABLE_NAME=self.ops.max_name_length()
        # self.MAX_INDEX_NAME=self.MAX_TABLE_NAME - 2
        # self.MAX_CONSTRAINT_NAME=self.ops.max_name_length()
        # self.MAX_SEQNAME=self.MAX_TABLE_NAME - 3
        #=======================================================================

        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.introspection = DatabaseIntrospection(self)
        if _DJANGO_VERSION >= 12:
            self.validation = BaseDatabaseValidation(self)
        else:
            self.validation = BaseDatabaseValidation()

        self.connection = None
        self.owner = None

    def _cursor(self):
        new_conn = False
        settings_dict = self.settings_dict
        #=======================================================================
        # DSN=eslemien;HOST=localhost;DB=eslemien;UID=jyp;PWD=jyp;PORT=50000
        #=======================================================================
        db_str, user_str, passwd_str, port_str = None, None, "", None
        dual_str='DUAL'
        
        # Get OpenEdge internal Db Codepage (default iso8859-1) 
        if 'CPINTERNAL' in settings_dict:
            self.oecpinternal = settings_dict['CPINTERNAL']
            
        if 'TYPECNX' in settings_dict:
            if 'DSN' in settings_dict['TYPECNX']:
                #===================================================================
                # DSN
                #===================================================================
                
                typecnx_str = 'DSN=%s'%settings_dict['TYPECNX']['DSN']

            elif 'DRIVER' in settings_dict['TYPECNX']:
                #===================================================================
                # DRIVER
                #===================================================================
                typecnx_str = 'DRIVER={%s}'%settings_dict['TYPECNX']['DRIVER']        
        
            
        #===================================================================
        # DUAL TABLE
        #===================================================================
        if 'DUALTABLE' in settings_dict:
            dual_str = settings_dict['DUALTABLE']
                
        #===================================================================
        # Default Schema
        #===================================================================
        if settings_dict['DEFAULTSCHEMA']:
            defschema_str = settings_dict['DEFAULTSCHEMA']
        else:
            defschema_str = settings_dict['USER']
                
                
        if _DJANGO_VERSION >= 12:
            options = settings_dict['OPTIONS']
            if settings_dict['NAME']:
                db_str = settings_dict['NAME']
                
            if settings_dict['HOST']:
                host_str = settings_dict['HOST']
            else:
                host_str = 'localhost'
            if settings_dict['USER']:
                user_str = settings_dict['USER']
                
                
            if settings_dict['PASSWORD']:
                passwd_str = settings_dict['PASSWORD']
            if settings_dict['PORT']:
                port_str = settings_dict['PORT']
            
            self.introspection.uid = defschema_str
            self.owner = defschema_str
        else:
            options = settings_dict['DATABASE_OPTIONS']
                            
            if settings_dict['DATABASE_NAME']:
                db_str = settings_dict['DATABASE_NAME']
            if settings_dict['DATABASE_HOST']:
                host_str = settings_dict['DATABASE_HOST']
            else:
                host_str = 'localhost'
            if settings_dict['DATABASE_USER']:
                user_str = settings_dict['DATABASE_USER']
            if settings_dict['DATABASE_PASSWORD']:
                passwd_str = settings_dict['DATABASE_PASSWORD']
            if settings_dict['DATABASE_PORT']:
                port_str = settings_dict['DATABASE_PORT']
        if self.connection is None:
            new_conn = True
            if not db_str:
                from django.core.exceptions import ImproperlyConfigured
                raise ImproperlyConfigured('You need to specify NAME in your Django settings file.')

            
            connstr='%s;HOST=%s;DB=%s;UID=%s;PWD=%s;PORT=%s'%(typecnx_str,host_str,db_str,user_str,passwd_str,port_str)
            
            #import pdb; pdb.set_trace()
            self.connection = Database.connect(connstr)
            connection_created.send(sender=self.__class__)

        #=======================================================================
        # Set default schema
        #=======================================================================
        cursor = self.connection.cursor()
        cursor.execute("SET SCHEMA '%s'"%defschema_str)
        self.connection.commit()
        if len(cursor.execute("SELECT * FROM SYSPROGRESS.SYSTABLEs WHERE OWNER = '%s' AND TBL = '%s'"%(defschema_str,dual_str)).fetchall()) == 0 :        
            cursor.execute('CREATE TABLE "%s"."%s" (SEQACCESS integer)'%(defschema_str,dual_str))
            self.connection.commit()
            cursor.execute('INSERT INTO "%s"."%s" VALUES (1)'%(defschema_str,dual_str))
            self.connection.commit()
        
        return CursorWrapper(cursor, self.driver_needs_utf8, self.oecpinternal,defschema_str,self.ops,self.creation)

    ################# 20131007 #############################
    def leave_transaction_management(self):
        """
        Leaves transaction management for a running thread. A dirty flag is carried
        over to the surrounding block, as a commit will commit all changes, even
        those from outside. (Commits are on connection level.)
        """
        if self.transaction_state:
            del self.transaction_state[-1]
        else:
            raise TransactionManagementError(
                "This code isn't under transaction management")
        # We will pass the next status (after leaving the previous state
        # behind) to subclass hook.
        self._leave_transaction_management(self.is_managed())        
        if self._dirty:            
            self.rollback()
            raise TransactionManagementError(
                "Transaction managed block ended with pending COMMIT/ROLLBACK")
        self._dirty = False

    def set_dirty(self):
        """
        Sets a dirty flag for the current thread and code streak. This can be used
        to decide in a managed block of code to decide whether there are open
        changes waiting for commit.
        """
        #import pdb; pdb.set_trace()
        if self._dirty is not None:
            self._dirty = True
        else:
            raise TransactionManagementError("This code isn't under transaction "
                "management")
            
    def is_managed(self):
        """
        Checks whether the transaction manager is in manual or in auto state.
        """
        #import pdb; pdb.set_trace()
        if self.transaction_state:
            return self.transaction_state[-1]
        return settings.TRANSACTIONS_MANAGED
    
    def get_connection_params(self):
        return self.settings_dict

    def get_new_connection(self, conn_params):

        if 'CPINTERNAL' in conn_params :
            self.oecpinternal = conn_params['CPINTERNAL']
            
        if 'TYPECNX' in conn_params :
            if 'DSN' in conn_params['TYPECNX']:
                #===================================================================
                # DSN
                #===================================================================
                
                typecnx_str = 'DSN=%s'%conn_params['TYPECNX']['DSN']

            elif 'DRIVER' in conn_params['TYPECNX']:
                #===================================================================
                # DRIVER
                #===================================================================
                typecnx_str = 'DRIVER={%s}'%conn_params['TYPECNX']['DRIVER']        

        #===================================================================
        # DUAL TABLE
        #===================================================================
        if 'DUALTABLE' in conn_params:
            dual_str = conn_params['DUALTABLE']
                
        #===================================================================
        # Default Schema
        #===================================================================
        if conn_params['DEFAULTSCHEMA']:
            defschema_str = conn_params['DEFAULTSCHEMA']
        else:
            defschema_str = conn_params['USER']

        options = conn_params['OPTIONS']
        if conn_params['NAME']:
            db_str = conn_params['NAME']
            
        if conn_params['HOST']:
            host_str = conn_params['HOST']
        else:
            host_str = 'localhost'
        if conn_params['USER']:
            user_str = conn_params['USER']
            
            
        if conn_params['PASSWORD']:
            passwd_str = conn_params['PASSWORD']
        if conn_params['PORT']:
            port_str = conn_params['PORT']
        
        self.introspection.uid = defschema_str
        self.owner = defschema_str
        if self.connection is None:
            new_conn = True
            if not db_str:
                from django.core.exceptions import ImproperlyConfigured
                raise ImproperlyConfigured('You need to specify NAME in your Django settings file.')

            
            connstr='%s;HOST=%s;DB=%s;UID=%s;PWD=%s;PORT=%s'%(typecnx_str,host_str,db_str,user_str,passwd_str,port_str)
            
            #import pdb; pdb.set_trace()
            newconnection = Database.connect(connstr)
            connection_created.send(sender=self.__class__)
            return newconnection

    def _set_autocommit(self, autocommit):
        self.autocommit = True

    def init_connection_state(self):
        return True

class CursorWrapper(object):
    """
    A wrapper around the pyodbc's cursor that takes in account a) some pyodbc
    DB-API 2.0 implementation and b) some common ODBC driver particularities.
    """
    def __init__(self, cursor, driver_needs_utf8,oecpinternal,defschema_str,ops,creation):
        self.cursor = cursor
        self.driver_needs_utf8 = driver_needs_utf8
        self.oecpinternal = oecpinternal
        self.last_sql = ''
        self.last_params = ()
        self.defaultSchema = defschema_str
        
        self.MAX_TABLE_NAME=ops.max_name_length()
        self.MAX_INDEX_NAME=self.MAX_TABLE_NAME - 2
        self.MAX_CONSTRAINT_NAME=ops.max_name_length()
        self.MAX_SEQNAME=self.MAX_TABLE_NAME - 3
        
        self.creation = creation
        self.ops = ops

    def format_sql(self, sql, n_params=None):
        
        ## 20190313 portage python 3.7 if self.driver_needs_utf8 and isinstance(sql, str):            
        ## 20190313 portage python 3.7     sql = sql.encode('utf-8')
        
        # pyodbc uses '?' instead of '%s' as parameter placeholder.
        if n_params is not None:
            sql = sql % tuple('?' * n_params)
        else:
            if '%s' in sql:
                sql = sql.replace('%s', '?')
            
        return sql

    def format_params(self, params):
        
        #import pdb; pdb.set_trace()
        
        fp = []
        
        for p in params:
            if isinstance(p, str):
                if self.driver_needs_utf8:                    
                    ## fp.append(p.encode('utf-8'))
                    ## 20200113 Portage Django 2 python 3
                    fp.append(p.encode('utf-8').decode('ascii'))
                else:
                    fp.append(p)
            elif isinstance(p, str):
                if self.driver_needs_utf8:
                    # fp.append(p.decode(self.oecpinternal).encode('utf-8'))
                    ## 20200113 Portage Django 2 python 3
                    fp.append(p.decode(self.oecpinternal).encode('utf-8').decode('ascii'))
                else:
                    fp.append(p)
            elif isinstance(p, type(True)):
                if p:
                    fp.append(1)
                else:
                    fp.append(0)
            else:
                fp.append(p)
        return tuple(fp)

    def execute(self, sql, params=()):
        #import pdb; pdb.set_trace()        
        self.last_sql = sql        
        sql = self.format_sql(sql, len(params))
        params = self.format_params(params)
        self.last_params = params
        
        ## print ('>>> -params - Execute ',sql,params)

        #=======================================================================
        # OpenEdge no ; at the end
        #=======================================================================
        if sql.endswith(';') is True:
            sql=sql[:-1]
        
        
        sqlUniqueIndex=None
        idSequence=None
        sql=sql.replace('\n','')
        
        if re.search('CREATE TABLE ',sql) is not None or re.search('ALTER TABLE ',sql) is not None:
            
            if re.search('CREATE TABLE ',sql) is not None :
                Statement='CREATE TABLE "'
            elif re.search('ALTER TABLE ',sql) is not None :
                Statement='ALTER TABLE "'
            
            motif='%s(?P<TName>\w+)"'%Statement    
            tn=re.search(motif, sql)
            if tn is not None:
                OETblName=tn.group('TName')[:self.MAX_TABLE_NAME]                
            
            motif='%s\w+"'%Statement
            sql=re.sub(motif,'', sql)
            if Statement == 'CREATE TABLE "':
                uniqueKw=re.search('(?P<uniqueClause>UNIQUE *\(.*\))', sql)
                if uniqueKw is not None:
                    
                    fidx=re.search('("\w+"[, ]*)+',uniqueKw.group('uniqueClause'))                    
                    FieldIdx=fidx.group().split(',')
                    indexName=self.ops.create_index_name(OETblName, FieldIdx, self.creation,self.MAX_INDEX_NAME,suffix="")                    
                    cols = ", ".join(FieldIdx)                    
                    sql=re.sub('(?P<uniqueClause>, *UNIQUE *\(".*"\))','', sql)
                    sqlUniqueIndex='CREATE UNIQUE INDEX %s ON "%s" (%s)'%(indexName,OETblName,cols)
                    
                    #=====================Old method ======================================
                    # fidx=re.search('("\w+"[, ]*)+',uniqueKw.group('uniqueClause'))
                    # if fidx is not None:
                    #     idxnum=1
                    #     FieldIdx=fidx.group().split(',')
                    #     sqlUniqueIndex='CREATE UNIQUE INDEX %s_%s ON "%s" ('%(OETblName[:self.MAX_INDEX_NAME],str(idxnum),OETblName)
                    #      
                    #     for fieldName in FieldIdx:
                    #         sqlUniqueIndex+='%s ,'%fieldName
                    #      
                    #     sqlUniqueIndex='%s)'%sqlUniqueIndex[:-1]
                    #===========================================================
                
                #idSequence='CREATE SEQUENCE PUB.ID_%s START WITH 0, INCREMENT BY 1, MINVALUE 0, NOCYCLE'%OETblName[:self.MAX_SEQNAME]
                
            
                
            sql='%s%s" %s'%(Statement,OETblName,sql)                
        
                
        #import pdb; pdb.set_trace()

        try:            
            rcode=self.cursor.execute(sql,params)            
            self.connection.commit()
        except  Exception as e:            
            #print 'OpenEdge Base %s  ::: values : %s ::: Sequence : %s ::: Unique Index : %s ' % (sql,params,idSequence,sqlUniqueIndex)
            print('OpenEdge base.py.execute()  Base %s  ::: values : %s :::  Unique Index : %s ' % (sql,params,sqlUniqueIndex))
            raise Database.DatabaseError(e)
                
        
        if sqlUniqueIndex is not None:
            self.cursor.execute(sqlUniqueIndex)
        self.connection.commit()        
        return rcode

    #def check_sql_string(self, sql, values):
    #    unique = "%PARAMETER%"
    #    sql = sql.replace("?", unique)
    #    #for v in values: sql = sql.replace(unique, repr(v.decode()), 1)
    #    for v in values: sql = sql.replace(unique, repr(v), 1)
    #    return sql
    
    def executemany(self, sql, params_list):        
        sql = self.format_sql(sql)
        # pyodbc's cursor.executemany() doesn't support an empty param_list
        if not params_list:
            if '?' in sql:
                return
        else:
            raw_pll = params_list
            params_list = [self.format_params(p) for p in raw_pll]
                       
        ## 20200122 With Django 3 this function is called but commy was missing

        try:      
            rcode=self.cursor.executemany(sql, params_list)
            self.connection.commit()
            return rcode
        except  Exception as e:            
            print('OpenEdge base.py.executemany() Base %s  ::: values : %s :::  Unique Index : %s ' % (sql,params_list))
            raise Database.DatabaseError(e)

    def format_results(self, rows):
        """
        Decode data coming from the database if needed and convert rows to tuples
        (pyodbc Rows are not sliceable).
        """
        #import pdb; pdb.set_trace()
        
        if not self.driver_needs_utf8:
            return tuple(rows)
        
        fr = []
        for row in rows:
            if isinstance(row, str):
                ##fr.append(row.decode(self.oecpinternal).encode('utf-8').decode('utf-8'))                
                ## 20200113 Adaptation Python 3 - Django 2
                fr.append(row.encode('ascii').decode(self.oecpinternal).encode('utf-8').decode('utf-8'))
            else:
                fr.append(row)
        return tuple(fr)

    def fetchone(self):
        row = self.cursor.fetchone()
        if row is not None:
            return self.format_results(row)
        return []

    def fetchmany(self, chunk):
        return [self.format_results(row) for row in self.cursor.fetchmany(chunk)]

    def fetchall(self):
        return [self.format_results(row) for row in self.cursor.fetchall()]

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        return getattr(self.cursor, attr)
    
    def __iter__(self):
        return iter(self.cursor)
    
    ############## 20131007 ################
    def set_dirty(self):
        if self.db.is_managed():
            self.db.set_dirty()

    ## 20191021 Add functions __enter__ et __exit__ for inspectdb works
    def __enter__(self):
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        # make sure the dbconnection gets closed
        self.cursor.close()            