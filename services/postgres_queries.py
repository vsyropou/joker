
column_names= lambda t: "SELECT column_name " \
                        "FROM information_schema.columns " \
                        "WHERE table_schema = 'public' "\
                        "AND table_name   = '%s' "%t
