SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE query LIKE 'SELECT merge_daemon%'
    AND datname = current_database();
DROP PUBLICATION IF EXISTS Shared_Pub;
DROP TRIGGER IF EXISTS shared_insert_trigger ON public.shared;
DROP TRIGGER IF EXISTS shared_wall_clock_trigger ON public.shared;
DROP RULE IF EXISTS data_insert_rule ON public.data;
DROP INDEX IF EXISTS public.shared_idx;
DROP INDEX IF EXISTS public.local_idx;
DROP INDEX IF EXISTS public.local_id_lts_idx;
ALTER TABLE IF EXISTS ONLY public.structurecontrol DROP CONSTRAINT structurecontrol_pkey;
ALTER TABLE IF EXISTS public.shared ALTER COLUMN seq DROP DEFAULT;
DROP SEQUENCE IF EXISTS public.wallclockseq;
DROP TABLE IF EXISTS public.structurecontrol;
DROP SEQUENCE IF EXISTS public.sitehybridlogicaltime;
DROP SEQUENCE IF EXISTS public.shared_seq_seq;
DROP VIEW IF EXISTS public.nested_view;
DROP VIEW IF EXISTS public.setrwtuple;
DROP VIEW IF EXISTS public.setrw;
DROP VIEW IF EXISTS public.setlwwtuple;
DROP VIEW IF EXISTS public.setlww;
DROP VIEW IF EXISTS public.setawtuple;
DROP VIEW IF EXISTS public.setaw;
DROP VIEW IF EXISTS public.registermvr;
DROP VIEW IF EXISTS public.registerlww;
DROP VIEW IF EXISTS public.maprwmvrtuple;
DROP VIEW IF EXISTS public.maprwmvr;
DROP VIEW IF EXISTS public.maplwwtuple;
DROP VIEW IF EXISTS public.maplww;
DROP VIEW IF EXISTS public.mapawmvrtuple;
DROP VIEW IF EXISTS public.mapawmvr;
DROP VIEW IF EXISTS public.mapawlwwtuple;
DROP VIEW IF EXISTS public.mapawlww;
DROP VIEW IF EXISTS public.listtuple;
DROP VIEW IF EXISTS public.list;
DROP VIEW IF EXISTS public.counter;
DROP VIEW IF EXISTS public.allrows;
DROP VIEW IF EXISTS public._listunsorted;
DROP VIEW IF EXISTS public.data;
DROP VIEW IF EXISTS public.datalocal;
DROP VIEW IF EXISTS public.dataall;
DROP VIEW IF EXISTS public.localandshared;
DROP TABLE IF EXISTS public.shared;
DROP TABLE IF EXISTS public.local;
DROP FUNCTION IF EXISTS public.wait_for_replication();
DROP FUNCTION IF EXISTS public.unschedule_merge_daemon();
DROP FUNCTION IF EXISTS public.switch_write_mode(mode character varying);
DROP FUNCTION IF EXISTS public.switch_read_mode(mode character varying);
DROP FUNCTION IF EXISTS public.switch_list_id_generation(mode character varying);
DROP FUNCTION IF EXISTS public.shared_wall_clock_function();
DROP FUNCTION IF EXISTS public.shared_insert_function();
DROP FUNCTION IF EXISTS public.setrwget(id_ character varying);
DROP FUNCTION IF EXISTS public.setrwcontains(id_ character varying, elem_ character varying);
DROP FUNCTION IF EXISTS public.setrmv(id_ character varying, value_ character varying);
DROP FUNCTION IF EXISTS public.setlwwget(id_ character varying);
DROP FUNCTION IF EXISTS public.setlwwcontains(id_ character varying, elem_ character varying);
DROP FUNCTION IF EXISTS public.setclear(id_ character varying);
DROP FUNCTION IF EXISTS public.setawget(id_ character varying);
DROP FUNCTION IF EXISTS public.setawcontains(id_ character varying, elem_ character varying);
DROP FUNCTION IF EXISTS public.setadd(id_ character varying, value_ character varying);
DROP FUNCTION IF EXISTS public.schedule_merge_daemon(workers integer, delta double precision, max_batch_size integer);
DROP FUNCTION IF EXISTS public.rmv_referential_integrity(src character varying[], dst character varying);
DROP FUNCTION IF EXISTS public.reset_data();
DROP FUNCTION IF EXISTS public.replicate();
DROP FUNCTION IF EXISTS public.registerset(id_ character varying, value_ character varying);
DROP FUNCTION IF EXISTS public.registermvrget(id_ character varying);
DROP FUNCTION IF EXISTS public.registerlwwget(id_ character varying);
DROP FUNCTION IF EXISTS public.nsites();
DROP FUNCTION IF EXISTS public.nexttimestamp(id_ character varying);
DROP PROCEDURE IF EXISTS public.merge_partition(IN partition integer, IN num_partitions integer, IN max_batch_size integer);
DROP PROCEDURE IF EXISTS public.merge_daemon(IN workers integer, IN delta double precision, IN max_batch_size integer);
DROP FUNCTION IF EXISTS public.merge_batch(batch bigint[]);
DROP FUNCTION IF EXISTS public.merge(id_ character varying, key_ character varying, type_ "char", data_ character varying, site_ integer, lts_ public.vclock, pts_ public.hlc, op_ "char");
DROP FUNCTION IF EXISTS public.merge();
DROP FUNCTION IF EXISTS public.maprwmvrvalue(id_ character varying, key_ character varying);
DROP FUNCTION IF EXISTS public.maprwmvrget(id_ character varying);
DROP FUNCTION IF EXISTS public.maprwmvrcontains(id_ character varying, key_ character varying);
DROP FUNCTION IF EXISTS public.maprmv(id_ character varying, key_ character varying);
DROP FUNCTION IF EXISTS public.maplwwvalue(id_ character varying, key_ character varying);
DROP FUNCTION IF EXISTS public.maplwwget(id_ character varying);
DROP FUNCTION IF EXISTS public.maplwwcontains(id_ character varying, key_ character varying);
DROP FUNCTION IF EXISTS public.mapclear(id_ character varying);
DROP FUNCTION IF EXISTS public.mapawmvrvalue(id_ character varying, key_ character varying);
DROP FUNCTION IF EXISTS public.mapawmvrget(id_ character varying);
DROP FUNCTION IF EXISTS public.mapawmvrcontains(id_ character varying, key_ character varying);
DROP FUNCTION IF EXISTS public.mapawlwwvalue(id_ character varying, key_ character varying);
DROP FUNCTION IF EXISTS public.mapawlwwget(id_ character varying);
DROP FUNCTION IF EXISTS public.mapawlwwcontains(id_ character varying, key_ character varying);
DROP FUNCTION IF EXISTS public.mapadd(id_ character varying, key_ character varying, value_ character varying);
DROP FUNCTION IF EXISTS public.listrmv(id_ character varying, index_ bigint);
DROP FUNCTION IF EXISTS public.listprepend(id_ character varying, elem_ character varying);
DROP FUNCTION IF EXISTS public.listpoplast(id_ character varying);
DROP FUNCTION IF EXISTS public.listpopfirst(id_ character varying);
DROP FUNCTION IF EXISTS public.listgetlast(id_ character varying);
DROP FUNCTION IF EXISTS public.listgetfirst(id_ character varying);
DROP FUNCTION IF EXISTS public.listgetat(id_ character varying, index_ integer);
DROP FUNCTION IF EXISTS public.listget(id_ character varying);
DROP FUNCTION IF EXISTS public.listclear(id_ character varying);
DROP FUNCTION IF EXISTS public.listappend(id_ character varying, elem_ character varying);
DROP FUNCTION IF EXISTS public.listadd(id_ character varying, index_ bigint, elem_ character varying);
DROP FUNCTION IF EXISTS public.initsite(site_id_ integer);
DROP FUNCTION IF EXISTS public.initiallogicaltime();
DROP FUNCTION IF EXISTS public.handleop(id_ character varying, key_ character varying, type_ "char", data_ character varying, site_ integer, lts_ public.vclock, pts_ public.hlc, op_ "char");
DROP FUNCTION IF EXISTS public.currenttimemillis();
DROP FUNCTION IF EXISTS public.counterget(id_ character varying);
DROP FUNCTION IF EXISTS public.counterinc(id_ character varying, delta_ bigint);
DROP FUNCTION IF EXISTS public.counterdec(id_ character varying, delta_ bigint);
DROP FUNCTION IF EXISTS public.addremotesite(site_id_ integer, host_ character varying, port_ character varying, dbname_ character varying, user_ character varying, password_ character varying);
DROP FUNCTION IF EXISTS public.add_referential_integrity(src character varying[], dst character varying, addfunc character varying);
DROP FUNCTION IF EXISTS public._physicaltovirtualindex(id_ character varying, index_ bigint);
DROP FUNCTION IF EXISTS public._lastvirtualindex(id_ character varying);
DROP FUNCTION IF EXISTS public._is_operation_obsolete(id_ character varying, key_ character varying, lts_ public.vclock);
DROP FUNCTION IF EXISTS public._generatevirtualindexbetween(p1 character varying, p2 character varying);
DROP FUNCTION IF EXISTS public._firstvirtualindex(id_ character varying);
DROP FUNCTION IF EXISTS public._delete_past_ops(id_ character varying, key_ character varying, lts_ public.vclock);
DROP FUNCTION IF EXISTS public._access_elem(id_ character varying);

DO $$
DECLARE subscription varchar;
BEGIN
    FOR subscription IN SELECT subname FROM pg_subscription WHERE subname LIKE 'sub\_' || (SELECT siteId()) || '\_%' LOOP
        EXECUTE format('ALTER SUBSCRIPTION %s DISABLE', subscription);
        EXECUTE format('ALTER SUBSCRIPTION %s SET (slot_name = None)', subscription);
        EXECUTE 'DROP SUBSCRIPTION ' || subscription;
    END LOOP;
EXCEPTION 
    WHEN undefined_function THEN
        RAISE NOTICE 'Function siteId does not exist, skipping subscription drop.';
END
$$;

DO $$
DECLARE replication varchar;
        pid int;
BEGIN
    FOR replication, pid IN SELECT slot_name, active_pid FROM pg_replication_slots WHERE database = (SELECT current_database()) LOOP
        LOOP
            BEGIN
                PERFORM pg_terminate_backend(pid);
                PERFORM pg_drop_replication_slot(replication);
                EXIT;
            EXCEPTION
                WHEN object_in_use THEN
                    RAISE NOTICE 'Failed to remove replication slot %, trying again.', replication;
            END;
        END LOOP;
    END LOOP;
END
$$;

DROP EXTENSION IF EXISTS clocks;
DROP EXTENSION IF EXISTS list_ids;
DROP FUNCTION IF EXISTS vclock_lte;
DROP FUNCTION IF EXISTS vclock_max;
DROP FUNCTION IF EXISTS next_hlc;
DROP TABLE IF EXISTS public.clusterinfo;
DROP FUNCTION IF EXISTS public.siteid();
DROP FUNCTION IF EXISTS public.is_schema_ready();
DROP TABLE IF EXISTS public.schemaready;
DROP TYPE IF EXISTS public.vclock_and_hlc CASCADE;
DROP TYPE IF EXISTS public.mentrymvr CASCADE;
DROP TYPE IF EXISTS public.mentry CASCADE;
DROP TYPE IF EXISTS public.hlc CASCADE;
DROP DOMAIN IF EXISTS public.vclock CASCADE;
DROP EXTENSION IF EXISTS dblink;
DROP EXTENSION IF EXISTS pg_background;
