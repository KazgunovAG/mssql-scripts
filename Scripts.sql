  select @@SERVERNAME
  --Назначение: Производительность MS SQL.Вывод мониторинга. Масштабируемые данные по времени
  --Автор: Прилепа Б.А. - АБД
  --Создано: 12.01.2017. Изменено 28.05.2019
  --exec srv.GetScalling_monitor_db @t=1						--RAM - HDD (СТАТИСТИКА DESC)

  --exec srv.GetScalling_monitor_db @t=2						--(БД) Размеры баз данных, mdf и ldf файла
  --exec srv.GetScalling_monitor_db @t=22						--(БД) Свободное место в файлах mdf, ldf
  --exec srv.GetScalling_monitor_db @t=3						--(БД) RAM под БД
  --exec srv.GetScalling_monitor_db @t=5						--(БД) Затраты процессорного времени по базам данных (CPUtime)
  --exec srv.GetScalling_monitor_db @t=6,@DATABASE='Trucks',@status_session='running'			--(БД) Сессии на БД (можно и по имени хоста) --status: 'running','sleeping'
  --exec srv.GetScalling_monitor_db @t=7						--(БД) Текущие запросы выполняемые на сервере (sp_Locks)
  --exec srv.GetScalling_monitor_db @t=8			--(БД) Мониторинг блокировок и повисающих запросов, job-ов (sp_Locks 3)
  --exec srv.GetScalling_monitor_db @t=19                       --(БД) Кто активен и потребление ресурсов ([dbo].[sp_WhoIsActive_Admin])
  --exec srv.GetScalling_monitor_db @t=20,@DATABASE='',@tbl='transportmodel1cs'	--(БД) Статистика по запросам
  --exec srv.GetScalling_monitor_db @t=18,@DATABASE='MonopolySun',@type_desc_object='P',@Unloading=0 ,@TABLE='ms_Mail_Prepare&Send_CheckBookingMail'  --(БД) Статистика по запросам БД (хранимки)
  --exec Fortisadmin.srv.GetScalling_monitor_db @t=9,@DATABASE='OctopusDB',@DATE1='2019-05-28'			--(БЭКАПЫ PRD-SQL-SRV01-02) Мониторинг бэкапов (запросом)

  --exec srv.GetScalling_monitor_db @t=4,@DATABASE='MonopolySunTemp',@ON_ALL_DATABASE=0,@TABLE='external_transport_drivers'	--(TABLES) Размеры таблиц (+ обращения), данные в них и индексы (Если БД нет, то ничего не выведет)
  --exec srv.GetScalling_monitor_db @t=21,@TABLE='msTruckInOrder_OnOff_ToAgreement',@like=1 --(иногда возвращает пусто или не полный набор, особенности отображения в sys) --ПОИСК ИСПОЛЬЗОВАНИЯ ОБЪЕКТА ПО ВСЕМ БАЗАМ
  --exec srv.GetScalling_monitor_db @t=21,@TABLE='Order',@DATABASE='Trucks',@like=0 --(иногда возвращает пусто или не полный набор, особенности отображения в sys) --ПОИСК ИСПОЛЬЗОВАНИЯ ОБЪЕКТА ПО ВСЕМ БАЗАМ
  --exec srv.GetScalling_monitor_db @t=21,@DATABASE='MonopolySun',@like=0 --использование БД в хранимых объектах
  --exec srv.GetScalling_monitor_db @t=21,@DATABASE='MonopolySun',@job=1 --И джобы рассматриваем
  --exec srv.GetScalling_monitor_db @t=10,@DATABASE='WialonBuffer',@TABLE='booking_histories' 		--(TABLES) Суммарная cтатистика чтений и записей по таблицам
  --exec srv.GetScalling_monitor_db @t=17,@DATABASE='SRV'				--(БД) индексы! Последние чтения из таблиц, по всем таблицам БД
  --exec srv.GetScalling_monitor_db @t=17,@DATABASE='MonopolySun',@TABLE='msOrder_ToAgreement'			--(БД) индексы! Последние чтения из таблиц, без указания таблицы выводится по всем таблицам

  --exec srv.GetScalling_monitor_db @t=10						--(БД) Суммарная cтатистика по базам данных чтение/запись (индексы)
  --exec srv.GetScalling_monitor_db @t=16						--(БД) Суммарная cтатистика по базам данных чтение/запись (системные счетчики)

  --exec srv.GetScalling_monitor_db @t=11		--(JOBS) Данные по выполнению заданий на сервере (job-ов) msdb.dbo.GetStatisticJobs
  --exec srv.GetScalling_monitor_db @t=12		--(WAITS) Мониторинг статистики по ожиданиям /*Очистить статистику DBCC SQLPERF ('sys.dm_os_wait_stats', CLEAR);*/

  --exec srv.GetScalling_monitor_db @t=13,@DATABASE='MonopolySunTemp',@TABLE='Order'		--(INDEXES) Используемость индексов в БД, какие индексы можно убрать
  --exec srv.GetScalling_monitor_db @t=14,@DATABASE='MonopolySunTemp',@TABLE='transfer_Order'		--(INDEXES) Фрагментированность индексов
  --exec srv.GetScalling_monitor_db @t=14,@DATABASE='MonopolySunTemp',@TABLE='Order',@Index_Is_Detailed=1		--(INDEXES) Фрагментированность индексов
  --exec srv.GetScalling_monitor_db @t=15,@DATABASE='Trucks'			--(INDEXES) Перечень всех индексов

/*
Активные сессии
  @Mode - 0 вывод активных запросов без кода T-SQL и sql plan 1 окно
  @Mode - 1 блокируемый запрос и заблокированный 1 окно
  @Mode - 2 запрос инициирующий блокировку в том числе каскадную и окно @Mode - 1 (блокируемый и заблокированный запрос)
  @Mode - 3 три окна, сочетаниие @Mode 2 и 0, но в третьем окне дополнительно выводится полный текст запроса и sql plan
*/
exec [FortisAdmin].srv.Locks 3

--статистика по ожиданиям
exec FortisAdmin.srv.Get_Statistic_Waits

--зависимости
exec FortisAdmin.srv.Depend_objects @DB = 'FortisAdmin'

exec FortisAdmin.srv.GetScalling_monitor_db @t=2						--(БД) Размеры баз данных, mdf и ldf файла

--select @@SERVERNAME

--Список баз экземпляра
SELECT name, database_id, SUSER_SNAME(owner_sid) as username, owner_sid, create_date
		,'ALTER AUTHORIZATION ON DATABASE::'+name+' TO sa;', state, state_desc
		--,'use ['+name+'] select DB_NAME() AS DBName, name as tab_name from sys.tables where name like ''%_AccRg23676%'''
FROM sys.databases 
where database_id > 4
	--and SUSER_SNAME(owner_sid) is null
	--and upper(name) like upper('%dwh%')
order by name asc
;

--Список заданий
SELECT job_id, [name], enabled 
	  ,'exec msdb..sp_update_job @job_name = N'''+name+''', @enabled = 0;' as ddl_dis
FROM msdb.dbo.sysjobs;

--Список бэкапов
SELECT  @@Servername AS ServerName ,
        d.Name AS DBName ,
		b.name as BackupProvider,
		b.backup_start_date,
        b.Backup_finish_date,
        bmf.Physical_Device_name,
		round(b.compressed_backup_size/1024/1024, 0) as Mb
		,b.type
		,b.description
		,b.database_creation_date
FROM    sys.databases d
        INNER JOIN msdb..backupset b ON b.database_name = d.name
                                        --AND b.[type] = 'D'
        left JOIN msdb.dbo.backupmediafamily bmf ON b.media_set_id = bmf.media_set_id
WHERE b.backup_start_date > GETDATE()-90 
		and d.name = 'OctopusDB'
		--and b.type!= 'L'
ORDER BY b.backup_start_date DESC, d.NAME; 

		
--Файлы данных и используемое место
with fs AS
(
	SELECT  @@SERVERNAME AS Server,
			d.name AS DBName,
			create_date,		
			m.physical_name AS FileName,
			m.name as logicalName
			,coalesce(m.size, 1)*8/1024 as size_MB
			,coalesce(convert(bigint, m.max_size), 1)*8/1024 as max_size_mb
			,(m.size - CAST(FILEPROPERTY(m.name, 'SpaceUsed') AS INT))*8/1024 as free_mb
			,coalesce(m.growth, 1)*8/1024 as growth_mb
			,sum(coalesce(m.size, 1)*8/1024) over (PARTITION BY d.name) as summ_db_total_mb
			,sum(coalesce(convert(bigint, m.max_size), 1)*8/1024) over (PARTITION BY d.name) as summ_db_max_size_total_mb
			,sum(coalesce(m.size, 1)*8/1024) over (PARTITION BY d.name, m.type) as summ_db_size_file_type_mb
			,sum(coalesce(convert(bigint, m.max_size), 1)*8/1024) over (PARTITION BY d.name, m.type) as summ_db_max_size_file_type_mb
			,sum(coalesce(m.size, 1)*8/1024) over (PARTITION BY 1) as summ_mb_total
			,f.name as FileGroup
			,m.type_desc as file_type
			,m.state_desc as file_state
			--,'alter DATABASE ['+d.name+'] add file (name = N'''+m.name+''', filename = N'''+m.physical_name+''', size = 3072KB, maxsize = 4194304KB, FILEGROWTH = 1024KB) to filegroup ['+coalesce(f.name, 'no_filegroup')+'];' as ddl_add_file
			,'alter DATABASE ['+d.name+'] MODIFY FILE ( NAME = N'''+m.name+''', MAXSIZE = GB );' as ddl_modify_maxsize
			--,m.*
	FROM    sys.databases d
			JOIN sys.master_files m ON d.database_id = m.database_id
			left join sys.filegroups f on m.data_space_id = f.data_space_id
	WHERE   1=1
		--and d.name in ('BIT_UZ_NEW')
			--and upper(m.physical_name) like upper('%g:%')
			--and m.[type] = 0 -- data files only
			and d.name like '%fuel%'
)
select * from fs order BY summ_db_total_mb desc
; 

--Размер баз
exec sp_helpdb

--Анализ размера логов
DBCC SQLPERF(logspace)

exec fortisadmin.[dbo].[sp_WhoIsActive_Admin]

--Проверить свободное место
exec sys.xp_fixeddrives
/*
USE [master]
GO
ALTER DATABASE [OctopusDB] SET RECOVERY SIMPLE WITH NO_WAIT
GO
USE [OctopusDB]
GO
DBCC SHRINKFILE (N'OctopusDB_log' , 0, TRUNCATEONLY)
go
USE [master]
GO
ALTER DATABASE [OctopusDB] SET RECOVERY FULL WITH NO_WAIT
GO
USE [PRISTAV]
GO
DBCC SHRINKDATABASE(N'PKB_DWH_Production')
*/
--Size of tables
SELECT
	t.NAME AS TableName,
	s.Name AS SchemaName,
	p.rows AS RowCounts,
	SUM(a.total_pages) * 8/1024 AS TotalSpaceMB,
	SUM(a.used_pages) * 8/1024 AS UsedSpaceMB,
	(SUM(a.total_pages) - SUM(a.used_pages)) * 8/1024 AS UnusedSpaceMB
FROM sys.tables t
INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id
left outer join sys.objects o1 on t.object_id = o1.parent_object_id and o1.type = 'PK'
--WHERE lower(t.NAME) LIKE '%spxml_blobs%'-- AND t.is_ms_shipped = 0 AND i.OBJECT_ID > 255	
where o1.object_id is not null
GROUP BY t.Name, s.Name, p.Rows
ORDER BY SUM(a.total_pages) * 8/1024 desc

--Файлы и свободное место
USE master
GO 

CREATE TABLE #TMPFIXEDDRIVES ( DRIVE CHAR(1), MBFREE INT) 

INSERT INTO #TMPFIXEDDRIVES 
EXEC xp_FIXEDDRIVES 

CREATE TABLE #TMPSPACEUSED ( DBNAME VARCHAR(50), FILENME VARCHAR(50), SPACEUSED FLOAT) 

INSERT INTO #TMPSPACEUSED 
EXEC( 'sp_msforeachdb''use [?]; Select ''''?'''' DBName, Name FileNme, fileproperty(Name,''''SpaceUsed'''') SpaceUsed from sysfiles''') 

SELECT   C.DRIVE, 
         CASE  
           WHEN (C.MBFREE) > 1000 THEN CAST(CAST(((C.MBFREE) / 1024.0) AS DECIMAL(18,2)) AS VARCHAR(20)) + ' GB' 
           ELSE CAST(CAST((C.MBFREE) AS DECIMAL(18,2)) AS VARCHAR(20)) + ' MB' 
           END AS DISKSPACEFREE, 
         A.NAME AS DATABASENAME, 
         B.NAME AS FILENAME, 
         CASE B.TYPE  
           WHEN 0 THEN 'DATA' 
           ELSE TYPE_DESC 
           END AS FILETYPE, 
         CASE  
           WHEN (B.SIZE * 8 / 1024.0) > 1000 
           THEN CAST(CAST(((B.SIZE * 8 / 1024) / 1024.0) AS DECIMAL(18,2)) AS VARCHAR(20)) + ' GB' 
           ELSE CAST(CAST((B.SIZE * 8 / 1024.0) AS DECIMAL(18,2)) AS VARCHAR(20)) + ' MB' 
           END AS FILESIZE, 
         CAST((B.SIZE * 8 / 1024.0) - (D.SPACEUSED / 128.0) AS DECIMAL(15,2)) SPACEFREE, 
         B.PHYSICAL_NAME 
FROM     SYS.DATABASES A 
         JOIN SYS.MASTER_FILES B ON A.DATABASE_ID = B.DATABASE_ID 
         JOIN #TMPFIXEDDRIVES C  ON LEFT(B.PHYSICAL_NAME,1) = C.DRIVE 
         JOIN #TMPSPACEUSED D    ON A.NAME = D.DBNAME AND B.NAME = D.FILENME 
ORDER BY DISKSPACEFREE, 
         SPACEFREE DESC 
          
DROP TABLE #TMPFIXEDDRIVES 

DROP TABLE #TMPSPACEUSED 

--Просмотр статистики выполнения запросов
select top 100 qs.sql_handle, qt.text, 	qs.execution_count, qs.total_worker_time, qs.plan_handle, qs.last_worker_time, qs.min_worker_time, qs.max_worker_time, qs.total_physical_reads, qs.total_logical_reads, qs.total_rows, qtp.query_plan
--,qs.*
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
cross APPLY sys.dm_exec_text_query_plan(qs.plan_handle, qs.statement_start_offset, qs.statement_end_offset) qtp
--where qt.text like 'select top 1000%_Task18%join%_Task18%'
--where qt.text like 'select%_Task18%join%_Task18'
order BY qs.max_worker_time desc, qs.execution_count desc

--Статистика ожиданий
select t.*, round(t.waiting_tasks_count*100/t.total_cnt, 0) as prcnt from (select *, round(wait_time_ms/case when waiting_tasks_count > 0 then waiting_tasks_count else 1 end, 0) as avg_ms, sum(waiting_tasks_count) over (PARTITION by 1) as total_cnt, getdate() as sys_dt_ins from sys.dm_os_wait_stats ) t order BY waiting_tasks_count desc, wait_time_ms desc;

--Сделать снимок статистики ожиданий
--insert into dba.dbo.tab_dm_os_wait_stats
--select *, getdate() into dba.dbo.tab_dm_os_wait_stats from sys.dm_os_wait_stats;
select *, getdate() from sys.dm_os_wait_stats;

--Просмотреть снимки статитстик ожиданий
select t.*, round(t.waiting_tasks_count*100/t.total_cnt, 0) as prcnt from (select *, round(wait_time_ms/case when waiting_tasks_count > 0 then waiting_tasks_count else 1 end, 0) as avg_ms, sum(waiting_tasks_count) over (PARTITION by sys_dt_ins) as total_cnt from dba.dbo.tab_dm_os_wait_stats) t  WHERE waiting_tasks_count > 0 and wait_type in (N'MEMORY_ALLOCATION_EXT', N'MEMORY_ALLOCATION_EXT', N'OLEDB', N'RESERVED_MEMORY_ALLOCATION_EXT', N'RESERVED_MEMORY_ALLOCATION_EXT', N'SOS_SCHEDULER_YIELD', N'SLEEP_TASK', N'PAGEIOLATCH_SH', N'LOGMGR_QUEUE', N'SOS_SCHEDULER_YIELD', N'SLEEP_TASK', N'LOGMGR_QUEUE', N'LAZYWRITER_SLEEP', N'PAGEIOLATCH_SH', N'WRITELOG', N'BACKUPBUFFER', N'PAGEIOLATCH_EX', N'ASYNC_NETWORK_IO', N'PAGELATCH_EX', N'LAZYWRITER_SLEEP', N'BACKUPIO', N'WRITELOG', N'IO_COMPLETION', N'DIRTY_PAGE_POLL', N'BACKUPBUFFER', N'DIRTY_PAGE_POLL', N'IO_COMPLETION', N'PAGEIOLATCH_EX', N'SLEEP_BPOOL_FLUSH', N'BACKUPIO', N'DISPATCHER_QUEUE_SEMAPHORE', N'HADR_CLUSAPI_CALL', N'ASYNC_NETWORK_IO', N'SLEEP_BPOOL_FLUSH', N'HADR_CLUSAPI_CALL', N'DISPATCHER_QUEUE_SEMAPHORE', N'HADR_FILESTREAM_IOMGR_IOCOMPLETION', N'HADR_FILESTREAM_IOMGR_IOCOMPLETION', N'PAGELATCH_EX', N'PREEMPTIVE_XE_CALLBACKEXECUTE', N'PREEMPTIVE_OS_AUTHENTICATIONOPS', N'PREEMPTIVE_XE_CALLBACKEXECUTE', N'PREEMPTIVE_XE_SESSIONCOMMIT', N'PREEMPTIVE_OS_CRYPTOPS', N'PREEMPTIVE_OS_AUTHENTICATIONOPS', N'PREEMPTIVE_XE_SESSIONCOMMIT', N'LOGBUFFER', N'BROKER_TO_FLUSH', N'PAGELATCH_SH', N'BROKER_TO_FLUSH', N'PREEMPTIVE_OS_CRYPTOPS', N'SP_SERVER_DIAGNOSTICS_SLEEP', N'SP_SERVER_DIAGNOSTICS_SLEEP', N'PREEMPTIVE_OS_AUTHORIZATIONOPS', N'XE_TIMER_EVENT', N'PREEMPTIVE_OS_QUERYREGISTRY', N'PREEMPTIVE_XE_TARGETINIT', N'PREEMPTIVE_XE_TARGETFINALIZE', N'PREEMPTIVE_OS_CRYPTACQUIRECONTEXT', N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', N'XE_TIMER_EVENT', N'SLEEP_BUFFERPOOL_HELPLW', N'PREEMPTIVE_OS_QUERYREGISTRY', N'CHECKPOINT_QUEUE', N'PREEMPTIVE_XE_TARGETFINALIZE', N'PREEMPTIVE_XE_TARGETINIT', N'PAGELATCH_UP', N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', N'BROKER_TASK_STOP', N'REQUEST_FOR_DEADLOCK_SEARCH', N'TRACEWRITE', N'PREEMPTIVE_OS_AUTHORIZATIONOPS', N'PAGEIOLATCH_UP', N'CHECKPOINT_QUEUE', N'BROKER_TASK_STOP', N'REQUEST_FOR_DEADLOCK_SEARCH', N'PREEMPTIVE_OS_CRYPTACQUIRECONTEXT', N'PREEMPTIVE_OS_WRITEFILE', N'PAGELATCH_SH', N'PREEMPTIVE_OS_QUERYCONTEXTATTRIBUTES', N'PREEMPTIVE_OS_REVERTTOSELF', N'PREEMPTIVE_OS_DELETESECURITYCONTEXT', N'PAGEIOLATCH_UP', N'PREEMPTIVE_OS_QUERYCONTEXTATTRIBUTES', N'PREEMPTIVE_OS_REVERTTOSELF', N'PREEMPTIVE_OS_DELETESECURITYCONTEXT', N'PREEMPTIVE_OS_WRITEFILE', N'PREEMPTIVE_OLEDBOPS', N'PREEMPTIVE_XE_GETTARGETSTATE', N'PREEMPTIVE_XE_GETTARGETSTATE', N'XE_DISPATCHER_WAIT', N'PAGELATCH_UP', N'XE_DISPATCHER_WAIT', N'BACKUPTHREAD', N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', N'PREEMPTIVE_OLEDBOPS', N'LCK_M_X', N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', N'WRITE_COMPLETION', N'PREEMPTIVE_OS_REPORTEVENT', N'LOGBUFFER', N'BACKUPTHREAD') order BY /*t.waiting_tasks_count desc, */t.wait_type, t.sys_dt_ins;

--просмотр системных статистик
SELECT sp.stats_id,        name,        filter_definition,        last_updated,        rows,        rows_sampled,        steps,        unfiltered_rows,        modification_counter
FROM sys.stats AS stat
     CROSS APPLY sys.dm_db_stats_properties(stat.object_id, stat.stats_id) AS sp
WHERE 1=1
	--and stat.object_id = OBJECT_ID('HumanResources.Employee')
	and modification_counter > 0
	and last_updated < getdate() - 1
order by last_updated
	;

--Отбор статистик для обновления
;with st AS(
	select DISTINCT 
	obj.[object_id]
	, obj.[create_date]
	, OBJECT_SCHEMA_NAME(obj.[object_id]) as [SchemaName]
	, obj.[name] as [ObjectName]
	, CAST(
			(
			   --общее число страниц, зарезервированных в секции (по 8 КБ на 1024 поделить=поделить на 128)
				SELECT SUM(ps2.[reserved_page_count])/128.
				from sys.dm_db_partition_stats as ps2
				where ps2.[object_id] = obj.[object_id]
			) as numeric (38,2)
		  ) as [ObjectSizeMB] --размер объекта в МБ
	, s.[stats_id]
	, s.[name] as [StatName]
	, sp.[last_updated]
	, i.[index_id]
	, i.[type_desc]
	, i.[name] as [IndexName]
	, ps.[row_count]
	, s.[has_filter]
	, s.[no_recompute]
	, sp.[rows]
	, sp.[rows_sampled]
	--кол-во изменений вычисляется как:
	--сумма общего кол-ва изменений в начальном столбце статистики с момента последнего обновления статистики
	--и разности приблизительного кол-ва строк в секции и общего числа строк в таблице или индексированном представлении при последнем обновлении статистики
	, sp.[modification_counter]+ABS(ps.[row_count]-sp.[rows]) as [ModificationCounter]
	--% количества строк, выбранных для статистических вычислений,
	--к общему числу строк в таблице или индексированном представлении при последнем обновлении статистики
	, NULLIF(CAST( sp.[rows_sampled]*100./sp.[rows] as numeric(18,3)), 100.00) as [ProcSampled]
	--% общего кол-ва изменений в начальном столбце статистики с момента последнего обновления статистики
	--к приблизительному количество строк в секции
	, CAST(sp.[modification_counter]*100./(case when (ps.[row_count]=0) then 1 else ps.[row_count] end) as numeric (18,3)) as [ProcModified]
	--Вес объекта:
	--[ProcModified]*десятичный логарифм от приблизительного кол-ва строк в секции
	, CAST(sp.[modification_counter]*100./(case when (ps.[row_count]=0) then 1 else ps.[row_count] end) as numeric (18,3))
								* case when (ps.[row_count]<=10) THEN 1 ELSE LOG10 (ps.[row_count]) END as [Func]
	--было ли сканирование:
	--общее количество строк, выбранных для статистических вычислений, не равно
	--общему числу строк в таблице или индексированном представлении при последнем обновлении статистики
	, CASE WHEN sp.[rows_sampled]<>sp.[rows] THEN 0 ELSE 1 END as [IsScanned]
	, tbl.[name] as [ColumnType]
	, s.[auto_created]	
	from sys.objects as obj
	inner join sys.stats as s on s.[object_id] = obj.[object_id]
	left outer join sys.indexes as i on i.[object_id] = obj.[object_id] and (i.[name] = s.[name] or i.[index_id] in (0,1) 
					and not exists(select top(1) 1 from sys.indexes i2 where i2.[object_id] = obj.[object_id] and i2.[name] = s.[name]))
	left outer join sys.dm_db_partition_stats as ps on ps.[object_id] = obj.[object_id] and ps.[index_id] = i.[index_id]
	outer apply sys.dm_db_stats_properties (s.[object_id], s.[stats_id]) as sp
	left outer join sys.stats_columns as sc on s.[object_id] = sc.[object_id] and s.[stats_id] = sc.[stats_id]
	left outer join sys.columns as col on col.[object_id] = s.[object_id] and col.[column_id] = sc.[column_id]
	left outer join sys.types as tbl on col.[system_type_id] = tbl.[system_type_id] and col.[user_type_id] = tbl.[user_type_id]
	where obj.[type_desc] <> 'SYSTEM_TABLE'
	)
	SELECT *
	FROM st
	WHERE NOT (st.[row_count] = 0 AND st.[last_updated] IS NULL)--если нет данных и статистика не обновлялась
		--если нечего обновлять
		AND NOT (st.[row_count] = st.[rows] AND st.[row_count] = st.[rows_sampled] AND st.[ModificationCounter]=0)
		--если есть что обновлять (и данные существенно менялись)
		AND ((st.[ProcModified]>=10.0) OR (st.[Func]>=10.0) OR (st.[ProcSampled]<=50))
;

--Columns description
SELECT     c.name 'Column Name',    t.Name 'Data type',    c.max_length 'Max Length',    c.precision ,    c.scale ,    c.is_nullable,    ISNULL(i.is_primary_key, 0) 'Primary Key'
FROM        sys.columns c
INNER JOIN     sys.types t ON c.user_type_id = t.user_type_id
LEFT OUTER JOIN     sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
LEFT OUTER JOIN     sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
;

--columns with PK
with c as
(
	select o.object_id, cu.CONSTRAINT_NAME, o.name, o.type, o.type_desc, c.TABLE_SCHEMA, c.COLUMN_NAME, c.DATA_TYPE, c.ORDINAL_POSITION, c.IS_NULLABLE, c.CHARACTER_MAXIMUM_LENGTH, c.COLUMN_DEFAULT, columnproperty(o.object_id, c.COLUMN_NAME, 'IsIdentity') as Is_Identity
			,case when cu.CONSTRAINT_NAME is not null then 1 else 0 end as is_pk
	from sys.objects o
	left outer join INFORMATION_SCHEMA.COLUMNS c on o.name = c.TABLE_NAME
	left outer join sys.objects o1 on o.object_id = o1.parent_object_id and o1.type = 'PK'
	left outer join INFORMATION_SCHEMA.KEY_COLUMN_USAGE cu on o1.name = cu.CONSTRAINT_NAME and c.COLUMN_NAME = cu.COLUMN_NAME
	where 1=1
	and o.type = 'U'
	--and columnproperty(o.object_id, c.COLUMN_NAME, 'IsIdentity') = 1
)
--select * from c where c.object_id = 555149023 and is_pk = 1
--select sum(is_pk) as cnt_pk, object_id from c group by object_id order by sum(is_pk) desc
select c.DATA_TYPE, count(*) as cnt from c where is_pk = 1 group by c.DATA_TYPE
;

--Найти пользователей
SELECT * FROM sys.syslogins l where upper(name) like upper('%dwh%')



--Уровен изоляции транзакций
SELECT CASE transaction_isolation_level 
WHEN 0 THEN 'Unspecified' 
WHEN 1 THEN 'ReadUncommitted' 
WHEN 2 THEN 'ReadCommitted' 
WHEN 3 THEN 'Repeatable' 
WHEN 4 THEN 'Serializable' 
WHEN 5 THEN 'Snapshot' END AS TRANSACTION_ISOLATION_LEVEL 
FROM sys.dm_exec_sessions 
where session_id = @@SPID

--хранимки с текстом
SELECT  @@Servername AS ServerName ,
        DB_NAME() AS DB_Name ,
        o.name AS 'ObjName' ,
        o.[type] ,
        o.Create_date ,
        sm.[definition] AS 'Stored Procedure script'
FROM    sys.objects o
        INNER JOIN sys.sql_modules sm ON o.object_id = sm.object_id
WHERE   1=1 --o.[type] in ('P', 'V', 'T') -- Stored Procedures 
	and o.name in ('idq_sp_create_address', 'idq_sp_create_Debt', 'idq_sp_create_Debt_Info', 'idq_sp_create_DebtGuarantor', 'idq_sp_create_Document', 'idq_sp_create_Person', 'idq_sp_create_phone', 'idq_person_details_view', 'idq_v_dict', 'Dict_name', 'dict')
        -- AND sm.[definition] LIKE '%insert%'
        -- AND sm.[definition] LIKE '%update%'
        -- AND sm.[definition] LIKE '%delete%'
        -- AND sm.[definition] LIKE '%tablename%'
ORDER BY o.type, o.name;

--Мониторинг спомощью событий
create event notification [blocked_threshold_exceeded] 
    on server for BLOCKED_PROCESS_REPORT 
    to service N'events', N'current database'; 


--collation of instance
select SERVERPROPERTY('collation');
--available collations
SELECT Name, Description FROM fn_helpcollations()  WHERE upper(name) like UPPER('latin%gen%ci%');


--Проверка баз на ошибки и исправление
/*
DBCC CHECKDB('bit_fin')

ALTER DATABASE [dba] SET single_user WITH ROLLBACK IMMEDIATE;

ALTER DATABASE [dba] SET emergency;

DBCC CHECKDB (N'bit_fin', REPAIR_ALLOW_DATA_LOSS) WITH ALL_ERRORMSGS, NO_INFOMSGS;

DBCC CHECKDB('dba') WITH NO_INFOMSGS, ALL_ERRORMSGS

ALTER DATABASE [Alerting] SET ONLINE;

ALTER DATABASE [dba] SET multi_user WITH ROLLBACK IMMEDIATE;

ALTER DATABASE [PKB_DWH_Production] SET OFFLINE;

sp_detach_db 'dba', true

sp_attach_db 'dba'
*/

--Создать Связанный сервер
USE [master]
GO
EXEC master.dbo.sp_addlinkedserver @server = N'prd-flpg-01', @srvproduct=N'SQL Server'
go
EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname = N'prd-flpg-01', @locallogin = NULL , @useself = N'False', @rmtuser = N'fuel_owner', @rmtpassword = N'wH!QNW9(A3nH'
GO
--Для готового драйвера ODBC
USE [master]
GO
EXEC master.dbo.sp_addlinkedserver @server = N'PRD-FLPG-01', @srvproduct=N'prd-flpg-01', @provider=N'MSDASQL', @datasrc=N'prd-flpg-01'

GO
EXEC master.dbo.sp_serveroption @server=N'PRD-FLPG-01', @optname=N'collation compatible', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'PRD-FLPG-01', @optname=N'data access', @optvalue=N'true'
GO
EXEC master.dbo.sp_serveroption @server=N'PRD-FLPG-01', @optname=N'dist', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'PRD-FLPG-01', @optname=N'pub', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'PRD-FLPG-01', @optname=N'rpc', @optvalue=N'true'
GO
EXEC master.dbo.sp_serveroption @server=N'PRD-FLPG-01', @optname=N'rpc out', @optvalue=N'true'
GO
EXEC master.dbo.sp_serveroption @server=N'PRD-FLPG-01', @optname=N'sub', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'PRD-FLPG-01', @optname=N'connect timeout', @optvalue=N'0'
GO
EXEC master.dbo.sp_serveroption @server=N'PRD-FLPG-01', @optname=N'collation name', @optvalue=null
GO
EXEC master.dbo.sp_serveroption @server=N'PRD-FLPG-01', @optname=N'lazy schema validation', @optvalue=N'false'
GO
EXEC master.dbo.sp_serveroption @server=N'PRD-FLPG-01', @optname=N'query timeout', @optvalue=N'0'
GO
EXEC master.dbo.sp_serveroption @server=N'PRD-FLPG-01', @optname=N'use remote collation', @optvalue=N'true'
GO
EXEC master.dbo.sp_serveroption @server=N'PRD-FLPG-01', @optname=N'remote proc transaction promotion', @optvalue=N'false'
GO
USE [master]
GO
EXEC master.dbo.sp_addlinkedsrvlogin @rmtsrvname = N'PRD-FLPG-01', @locallogin = NULL , @useself = N'False'
GO