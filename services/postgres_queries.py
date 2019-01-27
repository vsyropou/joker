
# get column names of a table
column_names = lambda t: "SELECT column_name " \
                         "FROM information_schema.columns " \
                         "WHERE table_schema = 'public' "\
                         "AND table_name   = '%s' "%t

# create embedings table 
create_embs_table = lambda nam, cnam, ctyp: "CREATE TABLE %s"%(nam) + \
                                            " (id SERIAL PRIMARY KEY, " + \
                                            ", ".join([' '.join([n,t,'NOT NULL ']) for n,t in zip(cnam,ctyp)]) +\
                                            ');'
# copy csv file to table
copy_csv_to_table = lambda tbnam, clnams, csvfile: "COPY %s (%s) "\
                                                   "FROM '%s' "\
                                                   "DELIMITER ',' "\
                                                   "CSV"%(tbnam,', '.join(clnams),csvfile)
