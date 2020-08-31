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

exec srv.GetScalling_monitor_db @t=2						--(БД) Размеры баз данных, mdf и ldf файла

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
			,coalesce(m.growth, 1)*8/1024 as growth_mb
			,sum(coalesce(m.size, 1)*8/1024) over (PARTITION BY d.name) as summ_db_total_mb
			,sum(coalesce(convert(bigint, m.max_size), 1)*8/1024) over (PARTITION BY d.name) as summ_db_max_size_total_mb
			,sum(coalesce(m.size, 1)*8/1024) over (PARTITION BY d.name, m.type) as summ_db_size_file_type_mb
			,sum(coalesce(convert(bigint, m.max_size), 1)*8/1024) over (PARTITION BY d.name, m.type) as summ_db_max_size_file_type_mb
			,sum(coalesce(m.size, 1)*8/1024) over (PARTITION BY 1) as summ_mb_total
			,sum(coalesce(m.size, 1)*8/1024) over (PARTITION BY 1) - sum(coalesce(CAST(FILEPROPERTY(m.name, 'SpaceUsed') AS INT), 1)*8/1024) over (PARTITION BY 1) as summ_free_mb_total
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

SELECT DB_NAME() AS DbName, 
    name AS FileName, 
    type_desc,
    size/128.0 AS CurrentSizeMB,  
    size/128.0 - CAST(FILEPROPERTY(name, 'SpaceUsed') AS INT)/128.0 AS FreeSpaceMB
FROM sys.database_files
WHERE type IN (0,1);

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



--select c.*, sum(TotalSpaceMB) as TotalSpaceMB_sum, sum(UsedSpaceMB) as UsedSpaceMB_sum, sum(UnusedSpaceMB) as UnusedSpaceMB_sum
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
	from FortisAdmin.sys.objects o
	left outer join FortisAdmin.INFORMATION_SCHEMA.COLUMNS c on o.name = c.TABLE_NAME
	left outer join FortisAdmin.sys.objects o1 on o.object_id = o1.parent_object_id and o1.type = 'PK'
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


select object_name(s.object_id, database_id),  avg_fragment_size_in_pages, fragment_count, avg_fragmentation_in_percent, s.*
from sys.dm_db_index_physical_stats(DB_ID(N'ustupki_nsv'), /*OBJECT_ID(N'AERdb')*/NULL, NULL, NULL , 'DETAILED') s
--inner join sys.indexes i on s.object_id = i.object_id
where 1=1 
     	--and avg_fragmentation_in_percent > 5
		--and avg_fragmentation_in_percent < 30
		and avg_fragmentation_in_percent >= 30
		--and object_name(s.object_id, database_id)!= 'interaction_activity_flat_table'


select tab_name, 'use [AERdb] ALTER INDEX ALL ON dbo.'+tab_name+' REORGANIZE;' as ddl from
(
	select distinct object_name(object_id, database_id) as tab_name, avg_fragmentation_in_percent
	from sys.dm_db_index_physical_stats(DB_ID(N'BIT_FIN'), /*OBJECT_ID(N'AERdb')*/NULL, NULL, NULL , NULL)
	where 1=1
		and avg_fragmentation_in_percent > 5
		and avg_fragmentation_in_percent < 30
		--and object_name(object_id, database_id)!= 'interaction_activity_flat_table'
) t

select tab_name, 'use [AERdb] ALTER INDEX ALL ON dbo.'+tab_name+' REBUILD;' as ddl from
(
	select distinct object_name(object_id, database_id) as tab_name
	from sys.dm_db_index_physical_stats(DB_ID(N'AERdb'), /*OBJECT_ID(N'AERdb')*/NULL, NULL, NULL , 'DETAILED')
	where 1=1
		and avg_fragmentation_in_percent >= 30
		--and object_name(object_id, database_id)!= 'interaction_activity_flat_table'
)


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