# -*- coding: utf-8 -*-

#===============================================================================
# from constants import MAX_CONSTRAINT_NAME    
# from constants import MAX_INDEX_NAME
# from constants import MAX_TABLE_NAME
# from constants import MAX_SEQNAME
#===============================================================================

from django.db.models.sql import compiler
## 20190313 portage python 3 from itertools import izip
try:
    # Python 2
    from itertools import izip
except ImportError:
    # Python 3
    izip = zip


from django.db.utils import DatabaseError
from datetime import datetime
import re
from django.db.models.sql.datastructures import EmptyResultSet
# 20190313 portage python3 from django.utils.encoding import smart_str, smart_unicode
from django.utils.encoding import smart_str, smart_text
## 20170412 Django 1.10 from django.db.models.sql.constants import (SINGLE, MULTI, ORDER_DIR, GET_ITERATOR_CHUNK_SIZE)
from django.db.models.sql.constants import (
    CURSOR, GET_ITERATOR_CHUNK_SIZE, MULTI, NO_RESULTS, ORDER_DIR, SINGLE,
)


class SQLCompiler(compiler.SQLCompiler):
    def formatTableName(self,data):
        #import pdb; pdb.set_trace()
        if isinstance(data,list) is True:
            for i,v in enumerate(data):
                tv=v.split('"')
                for iv,vv in enumerate(tv):
                    tv[iv]=vv[:self.connection.ops.max_name_length()]
                data[i]='"'.join(tv)
            return data
        else :            
            tdata=data.split('"')
            for i,v in enumerate(tdata):
                #===============================================================
                # If where clause is IN (val,val...), or LIKE  be careful to not substring the IN clause
                #===============================================================
                if 'IN (' in v or ' LIKE ' in v:
                    if 'COLLATE' in v:
                        tdata[i]=re.sub('COLLATE (\w+) ','',v)
                    else:
                        tdata[i]=v
                else:
                    tdata[i]=v[:self.connection.ops.max_name_length()]
                #===============================================================
                # if 'IN (' not in v:
                #     tdata[i]=v[:MAX_TABLE_NAME]
                # else:
                #     tdata[i]=v
                #===============================================================
                    
            return '"'.join(tdata)
    
    def as_sql(self, with_limits=False, with_col_aliases=False, subquery=False):
        """
        Creates the SQL for this query. Returns the SQL string and list of
        parameters.

        If 'with_limits' is False, any limit/offset information is not included
        in the query.
        """
        self.subquery = subquery
        refcounts_before = self.query.alias_refcount.copy()
        try:
            extra_select, order_by, group_by = self.pre_sql_setup()
            distinct_fields = self.get_distinct()

            # This must come after 'select', 'ordering', and 'distinct' -- see
            # docstring of get_from_clause() for details.
            from_, f_params = self.get_from_clause()

            where, w_params = self.compile(self.where) if self.where is not None else ("", [])
            having, h_params = self.compile(self.having) if self.having is not None else ("", [])
            params = []
            result = ['SELECT']

            if self.query.distinct:
                result.append(self.connection.ops.distinct_sql(distinct_fields))

            out_cols = []
            col_idx = 1
            for _, (s_sql, s_params), alias in self.select + extra_select:
                if alias:
                    s_sql = '%s AS %s' % (s_sql, self.connection.ops.quote_name(alias))
                elif with_col_aliases:
                    s_sql = '%s AS %s' % (s_sql, 'Col%d' % col_idx)
                    col_idx += 1
                params.extend(s_params)
                out_cols.append(s_sql)

            result.append(', '.join(out_cols))

            result.append('FROM')
            result.extend(from_)
            params.extend(f_params)

            if where:
                result.append('WHERE %s' % where)
                params.extend(w_params)

            grouping = []
            for g_sql, g_params in group_by:
                grouping.append(g_sql)
                params.extend(g_params)
            if grouping:
                if distinct_fields:
                    raise NotImplementedError(
                        "annotate() + distinct(fields) is not implemented.")
                if not order_by:
                    order_by = self.connection.ops.force_no_ordering()
                result.append('GROUP BY %s' % ', '.join(grouping))

            if having:
                result.append('HAVING %s' % having)
                params.extend(h_params)

            if order_by:
                ordering = []
                for _, (o_sql, o_params, _) in order_by:
                    ordering.append(o_sql)
                    params.extend(o_params)
                result.append('ORDER BY %s' % ', '.join(ordering))

            if with_limits:
                #===================================================================
                # 20200127 Edras Pacolas contrib
                #
                # OpenEdge use TOP, not LIMIT
                # TOP and OFFSET/FETCH clauses are mutually exclusive
                #   - TOP cannot be used in a query that uses OFFSET or FETCH.
                #===================================================================
                if self.query.low_mark:
                    val = self.query.high_mark
                    if val is None:
                        val = self.connection.ops.no_limit_value()
                    result.append('OFFSET %d ROWS FETCH NEXT %d ROWS ONLY' % \
                                  (self.query.low_mark,
                                  (val - self.query.low_mark)))
                elif self.query.high_mark is not None:
                    result[0] +=' TOP %d' % (self.query.high_mark - self.query.low_mark)

                ### OE ADAPT if self.query.high_mark is not None:
                ### OE ADAPT     result.append('LIMIT %d' % (self.query.high_mark - self.query.low_mark))
                ### OE ADAPT if self.query.low_mark:
                ### OE ADAPT     if self.query.high_mark is None:
                ### OE ADAPT         val = self.connection.ops.no_limit_value()
                ### OE ADAPT         if val:
                ### OE ADAPT             result.append('LIMIT %d' % val)
                ### OE ADAPT     result.append('OFFSET %d' % self.query.low_mark)

            
            if self.query.select_for_update and self.connection.features.has_select_for_update:
                if self.connection.get_autocommit():
                    raise TransactionManagementError(
                        "select_for_update cannot be used outside of a transaction."
                    )

                # If we've been asked for a NOWAIT query but the backend does
                # not support it, raise a DatabaseError otherwise we could get
                # an unexpected deadlock.
                nowait = self.query.select_for_update_nowait
                if nowait and not self.connection.features.has_select_for_update_nowait:
                    raise DatabaseError('NOWAIT is not supported on this database backend.')
                result.append(self.connection.ops.for_update_sql(nowait=nowait))

            return ' '.join(result), tuple(params)
        finally:
            # Finally do cleanup - get rid of the joins we created above.
            self.query.reset_refcounts(refcounts_before)

    # def as_sql(self, with_limits=True, with_col_aliases=False):
    #     #import pdb; pdb.set_trace()
    #     """
    #     Creates the SQL for this query. Returns the SQL string and list of
    #     parameters.

    #     If 'with_limits' is False, any limit/offset information is not included
    #     in the query.
    #     """
    #     if with_limits and self.query.low_mark == self.query.high_mark:
    #         return '', ()

    #     self.pre_sql_setup()
    #     # After executing the query, we must get rid of any joins the query
    #     # setup created. So, take note of alias counts before the query ran.
    #     # However we do not want to get rid of stuff done in pre_sql_setup(),
    #     # as the pre_sql_setup will modify query state in a way that forbids
    #     # another run of it.
    #     self.refcounts_before = self.query.alias_refcount.copy()
        
    #     ## 20170412 Django 1.10 out_cols = self.get_columns(with_col_aliases)
    #     out_cols = self.get_default_columns(with_col_aliases)

    #     ## 20170412 Django 1.10 ordering, ordering_group_by = self.get_ordering()
    #     ordering, ordering_group_by = self.get_order_by()

    #     distinct_fields = self.get_distinct()

    #     # This must come after 'select', 'ordering' and 'distinct' -- see
    #     # docstring of get_from_clause() for details.
    #     from_, f_params = self.get_from_clause()

    #     qn = self.quote_name_unless_alias
                
    #     where, w_params = self.query.where.as_sql(qn=qn, connection=self.connection)
    #     having, h_params = self.query.having.as_sql(qn=qn, connection=self.connection)
    #     params = []
    #     for val in self.query.extra_select.itervalues():
    #         params.extend(val[1])

    #     result = ['SELECT']
        
    #     if self.query.distinct:
    #         distinct_fields=self.formatTableName(distinct_fields)
    #         result.append(self.connection.ops.distinct_sql(distinct_fields))
        
    #     out_cols= self.formatTableName(out_cols)
    #     result.append(', '.join(out_cols + self.query.ordering_aliases))
        

    #     result.append('FROM')
    #     from_ = self.formatTableName(from_)
    #     result.extend(from_)
    #     params.extend(f_params)

    #     if where:
    #         where=self.formatTableName(where)
    #         result.append('WHERE %s' % where)
    #         params.extend(w_params)

    #     grouping, gb_params = self.get_grouping(True)
    #     if grouping:
    #         if distinct_fields:
    #             raise NotImplementedError(
    #                 "annotate() + distinct(fields) not implemented.")
    #         if ordering:
    #             # If the backend can't group by PK (i.e., any database
    #             # other than MySQL), then any fields mentioned in the
    #             # ordering clause needs to be in the group by clause.
    #             if not self.connection.features.allows_group_by_pk:
    #                 for col, col_params in ordering_group_by:
    #                     if col not in grouping:
    #                         grouping.append(str(col))
    #                         gb_params.extend(col_params)
    #         else:
    #             ordering = self.connection.ops.force_no_ordering()
    #         result.append('GROUP BY %s' % ', '.join(grouping))
    #         params.extend(gb_params)

    #     if having:
    #         result.append('HAVING %s' % having)
    #         params.extend(h_params)

    #     if ordering:
    #         result.append('ORDER BY %s' % ', '.join(ordering))

    #     if with_limits:
    #         #===================================================================
    #         # OpenEdge use TOP, not LIMIT
    #         #===================================================================
    #         if self.query.high_mark is not None:
    #             result[0]+=' TOP %d' % (self.query.high_mark - self.query.low_mark)
    #         if self.query.low_mark:
    #             if self.query.high_mark is None:
    #                 val = self.connection.ops.no_limit_value()
    #                 if val:
    #                     result[0]+=' TOP %d' % val
    #             #result.append('OFFSET %d' % self.query.low_mark)

    #     if self.query.select_for_update and self.connection.features.has_select_for_update:
    #         # If we've been asked for a NOWAIT query but the backend does not support it,
    #         # raise a DatabaseError otherwise we could get an unexpected deadlock.
    #         nowait = self.query.select_for_update_nowait
    #         if nowait and not self.connection.features.has_select_for_update_nowait:
    #             raise DatabaseError('NOWAIT is not supported on this database backend.')
    #         result.append(self.connection.ops.for_update_sql(nowait=nowait))

    #     # Finally do cleanup - get rid of the joins we created above.
    #     self.query.reset_refcounts(self.refcounts_before)
        
    #     return ' '.join(result), tuple(params)

    def execute_sql(self, result_type=MULTI,chunked_fetch=None,chunk_size=None):
        """
        Run the query against the database and returns the result(s). The
        return value is a single data item if result_type is SINGLE, or an
        iterator over the results if the result_type is MULTI.

        result_type is either MULTI (use fetchmany() to retrieve all rows),
        SINGLE (only retrieve a single row), or None. In this last case, the
        cursor is returned if any query is executed, since it's used by
        subclasses such as InsertQuery). It's possible, however, that no query
        is needed, as the filters describe an empty set. In that case, None is
        returned, to avoid any unnecessary database interaction.
        """
        
        if not result_type:
            result_type = NO_RESULTS
        try:
            sql, params = self.as_sql()
            if not sql:
                raise EmptyResultSet
        except EmptyResultSet:
            if result_type == MULTI:
                return iter([])
            else:
                return

        ## 20170627 Force nolock pour les select
        if sql.split()[0] == "SELECT":
            sql+=" WITH (NOLOCK)"

        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, params)
        except Exception:
            cursor.close()
            raise

        if result_type == CURSOR:
            # Caller didn't specify a result_type, so just give them back the
            # cursor to process (and close).
            return cursor
        if result_type == SINGLE:
            try:
                val = cursor.fetchone()
                if val:
                    return val[0:self.col_count]
                return val
            finally:
                # done with the cursor
                cursor.close()
        if result_type == NO_RESULTS:
            cursor.close()
            return

        result = cursor_iter(
            cursor, self.connection.features.empty_fetchmany_value,
            self.col_count
        )
        if not self.connection.features.can_use_chunked_reads:            
            ## 20140412 Django 1.10
            return list(result)

            # try:
            #     # If we are using non-chunked reads, we return the same data
            #     # structure as normally, but ensure it is all read into memory
            #     # before going any further.
            #     return list(result)
            # finally:
            #     # done with the cursor
            #     cursor.close()
        return result

    # def execute_sql(self, result_type=MULTI):
    #     """
    #     Run the query against the database and returns the result(s). The
    #     return value is a single data item if result_type is SINGLE, or an
    #     iterator over the results if the result_type is MULTI.

    #     result_type is either MULTI (use fetchmany() to retrieve all rows),
    #     SINGLE (only retrieve a single row), or None. In this last case, the
    #     cursor is returned if any query is executed, since it's used by
    #     subclasses such as InsertQuery). It's possible, however, that no query
    #     is needed, as the filters describe an empty set. In that case, None is
    #     returned, to avoid any unnecessary database interaction.
    #     """
    #     try:
    #         sql, params = self.as_sql()
    #         #import pdb; pdb.set_trace()
    #         if not sql:
    #             raise EmptyResultSet
    #     except EmptyResultSet:
    #         if result_type == MULTI:
    #             return iter([])
    #         else:
    #             return

    #     cursor = self.connection.cursor()        
    #     cursor.execute(sql, params)

    #     if not result_type:
    #         return cursor
    #     if result_type == SINGLE:
    #         if self.query.ordering_aliases:
    #             return cursor.fetchone()[:-len(self.query.ordering_aliases)]
    #         return cursor.fetchone()

    #     # The MULTI case.
    #     if self.query.ordering_aliases:
    #         result = order_modified_iter(cursor, len(self.query.ordering_aliases),
    #                 self.connection.features.empty_fetchmany_value)
    #     else:
    #         result = iter((lambda: cursor.fetchmany(GET_ITERATOR_CHUNK_SIZE)),
    #                 self.connection.features.empty_fetchmany_value)
    #     if not self.connection.features.can_use_chunked_reads:
    #         # If we are using non-chunked reads, we return the same data
    #         # structure as normally, but ensure it is all read into memory
    #         # before going any further.
    #         return list(result)
    #     return result

def order_modified_iter(cursor, trim, sentinel):
    """
    Yields blocks of rows from a cursor. We use this iterator in the special
    case when extra output columns have been added to support ordering
    requirements. We must trim those extra columns before anything else can use
    the results, since they're only needed to make the SQL valid.
    """
    for rows in iter((lambda: cursor.fetchmany(GET_ITERATOR_CHUNK_SIZE)),
            sentinel):
        yield [r[:-trim] for r in rows]

class SQLInsertCompiler(SQLCompiler):
    def placeholder(self, field, val):
        if field is None:
            # A field value of None means the value is raw.
            return val
        elif hasattr(field, 'get_placeholder'):
            # Some fields (e.g. geo fields) need special munging before
            # they can be inserted.
            return field.get_placeholder(val, self.connection)
        else:
            # Return the common case for the placeholder
            return '%s'
        
    def as_sql(self):
        # We don't need quote_name_unless_alias() here, since these are all
        # going to be column names (so we can avoid the extra overhead).
        qn = self.connection.ops.quote_name
        opts = self.query.model._meta
        self.ID = None
        
        #import pdb; pdb.set_trace()
        cursor = self.connection.cursor()
        owner = self.connection.owner
        
        table_has_col_id = False
        curtable=opts.db_table[:self.connection.ops.max_name_length()]
        
        #import pdb; pdb.set_trace()
        #=======================================================================
        # Check if table has id col, it's used to emulate autoincrement col
        #=======================================================================
        table_has_col_id = self.connection.ops.has_id_col(curtable,cursor,owner)
        
        #======================20131102=================================================
        # if len(cursor.execute("select col from sysprogress.syscolumns where tbl = '%s' and owner = '%s' and col = 'id'"%(curtable,owner)).fetchall()) > 0 :
        #     table_has_col_id = True
        #=======================================================================
        
        result = ['INSERT INTO %s' % qn(curtable)]
        
        has_fields = bool(self.query.fields)
        fields = self.query.fields if has_fields else [opts.pk]
        
        lfields='(%s' % ', '.join([qn(f.column) for f in fields])
        
        #=======================================================================
        # Test if id col is provided , if not, we have to add it (Openedge does not support autoincrement field)
        #=======================================================================

        hasIdCol=True
        if re.search('"id"',lfields) is None and table_has_col_id is True:
            hasIdCol=False
            lfields+=',"id")'
        else:
            #import pdb; pdb.set_trace()
            lfields+=')'    

        #print('>>> as_sql compiler.py OE',lfields)           
        result.append(lfields)
        
        if has_fields:
            params = values = [
                [
                    f.get_db_prep_save(getattr(obj, f.attname) if self.query.raw else f.pre_save(obj, True), connection=self.connection)
                    for f in fields
                ]
                for obj in self.query.objs
            ]
        else:
            values = [[self.connection.ops.pk_default_value()] for obj in self.query.objs]
            params = [[]]
            fields = [None]
            
        can_bulk = (not any(hasattr(field, "get_placeholder") for field in fields) and
            not self.return_id and self.connection.features.has_bulk_insert)

        if can_bulk:
            placeholders = [["%s"] * len(fields)]
        else:
            placeholders = [
                [self.placeholder(field, v) for field, v in izip(fields, val)]
                for val in values
            ]
            
            params = self.connection.ops.modify_insert_params(placeholders, params)
        
        #import pdb; pdb.set_trace() 
        params = [
                    ## 20200121 Adapt Python3 smart_str(v) if isinstance(v, unicode) else v
                    ## smart_str(v) if isinstance(v, bytes) else v
                    v for v in params[0]
                ]     
        

        if hasIdCol is False and table_has_col_id is True and can_bulk is False:             
            #import pdb; pdb.set_trace()
            self.ID=self.connection.ops.get_autoinc_keyval(opts.db_table, 'id',self.connection.ops.max_name_length(),cursor)
            #===========================20131101========================================
            # cursor.execute('select id_%s.nextval from dual'%opts.db_table[:self.connection.ops.max_name_length()-3])
            # self.ID=cursor.fetchone()[0]
            #===================================================================
            params.append(self.ID)
                             
        if self.return_id and self.connection.features.can_return_id_from_insert:            
            #===================================================================
            # Transcode unicode to string (openedge issue)
            #===================================================================
            col = "%s.%s" % (qn(curtable), qn(opts.pk.column))
            result.append("VALUES (%s" % ", ".join(placeholders[0]))
            
            if hasIdCol is False and table_has_col_id is True:
                result[-1]+=',%'+'s)'
            else:
                result[-1]+=')'
            
            #import pdb; pdb.set_trace()
            return [(" ".join(result), tuple(params))]
        
        if can_bulk:            
            #import pdb; pdb.set_trace()
            self.bulk_load=True
            tabID=None            
            if hasIdCol is False and table_has_col_id is True:
                for i,v in enumerate(values):
                    values[i].append(self.connection.ops.get_autoinc_keyval(opts.db_table, 'id',self.connection.ops.max_name_length(),cursor))
                    #======================20131101=====================================
                    # values[i].append(cursor.execute('select id_%s.nextval from dual'%opts.db_table[:self.connection.ops.max_name_length()-3]).fetchone()[0])
                    #===========================================================
                    
                result.append(self.connection.ops.bulk_insert_sql(fields, len(values),OEid=1))
            else:    
                result.append(self.connection.ops.bulk_insert_sql(fields, len(values)))
            
            #return [(" ".join(result),[v for val in values for v in val])]   
            #import pdb; pdb.set_trace()         
            return [(" ".join(result),values)]
        else:            
            #self.bulk_load=False    
            result.append("VALUES (%s" % ", ".join(placeholders[0]))
            
            if hasIdCol is False:
                result[-1]+=',%'+'s)'
            else:
                result[-1]+=')'
            
            import pdb; pdb.set_trace()
            return [(" ".join(result), tuple(params))]       
            
            
    def execute_sql(self, return_id=False):        
        self.bulk_load=False
        assert not (return_id and len(self.query.objs) != 1)
        self.return_id = return_id
        cursor = self.connection.cursor()        
        sql_param=self.as_sql()
        
        if self.bulk_load is not True:
            for sql, params in sql_param:                            
                cursor.execute(sql, params)
        else:      
            cursor.executemany(sql_param[0][0],sql_param[0][1])
        
        #import pdb; pdb.set_trace()
        if not (return_id and cursor):
            ## 20200122 Adapt Django 3
            #return
            return ()

        if self.ID is not None:
            ## 20200122 Adapt Django 3
            #return self.ID
            return (self.ID,)
        if self.connection.features.can_return_id_from_insert:
            return self.connection.ops.fetch_returned_insert_id(cursor)
        return self.connection.ops.last_insert_id(cursor,
                self.query.model._meta.db_table, self.query.model._meta.pk.column)

class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    pass

class SQLDeleteCompiler(compiler.SQLDeleteCompiler,SQLCompiler):
    def as_sql(self):
        """
        Creates the SQL for this query. Returns the SQL string and list of
        parameters.
        """
        ## 20200121 Adapt Django 3 assert len([t for t in self.query.tables if self.query.alias_refcount[t] > 0]) == 1, \

        assert len([t for t in self.query.alias_map if self.query.alias_refcount[t] > 0]) == 1, \
            "Can only delete from one table at a time."

        qn = self.quote_name_unless_alias
        result = ['DELETE FROM %s' % qn(self.query.base_table)]
        where, params = self.compile(self.query.where)
        if where:
            result.append('WHERE %s' % where)
        return ' '.join(result), tuple(params)


        ## 20200121 Adapt Django 3  qn = self.quote_name_unless_alias
        ## 20200121 Adapt Django 3  result = ['DELETE FROM %s' % qn(self.query.tables[0])]
        ## 20200121 Adapt Django 3  where, params = self.compile(self.query.where)
        ## 20200121 Adapt Django 3  if where:
        ## 20200121 Adapt Django 3      result.append('WHERE %s' % where)
        ## 20200121 Adapt Django 3  return ' '.join(result), tuple(params)

# class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    
#     #===========================================================================
#     # def _hasConstraints(self,curtable):
#     #     print '>>> Controle ',curtable
#     #     cursor = self.connection.cursor()
#     #     owner = self.connection.owner
#     #     hasConstraints = cursor.execute("select tblname from sysprogress.sys_ref_constrs where reftblname = '%s' and owner = '%s'"%(curtable,owner)).fetchall()
#     #     if len(hasConstraints) > 0:
#     #         print '>>>',hasConstraints
#     #         
#     #===========================================================================
#     def as_sql(self):
#         """
#         Creates the SQL for this query. Returns the SQL string and list of
#         parameters.
#         """
#         assert len(self.query.tables) == 1, \
#                 "Can only delete from one table at a time."
#         qn = self.quote_name_unless_alias
#         #=======================================================================
#         # self._hasConstraints(self.query.tables[0])
#         #=======================================================================
        
#         result = ['DELETE FROM %s' % qn(self.query.tables[0])]
#         #where, params = self.query.where.as_sql(qn=qn, connection=self.connection)
#         where, params = self.query.where.as_sql( compiler=compiler , connection=self.connection)
#         if where:
#             result.append('WHERE %s' % where)
#         ##DOTO: Delete after test
#         #=======================================================================
#         # print '>>>',result,params
#         # if result[0] == 'DELETE FROM "django_flatpage_sites"' :
#         #     import pdb; pdb.set_trace()
#         #=======================================================================
#         return ' '.join(result), tuple(params)

def cursor_iter(cursor, sentinel, col_count):
    """
    Yields blocks of rows from a cursor and ensures the cursor is closed when
    done.
    """
    try:
        for rows in iter((lambda: cursor.fetchmany(GET_ITERATOR_CHUNK_SIZE)),
                         sentinel):
            yield [r[0:col_count] for r in rows]
    finally:
        cursor.close()        