/*
Stored procedure that generates a sql query from a given table or view, with column name in the first line.
This is useful to generate a BCP query with a header.

Output example:
	SELECT 'MyId',
		'MyLabel',
		'MyDate',
		'...'
	UNION ALL
	SELECT CAST(MyId AS NVARCHAR(4000)),
		CAST(MyLabel AS NVARCHAR(4000)),
		CONVERT(NVARCHAR(50), MyDate, 120)
	FROM MyDb.dbo.MyTable
	WHERE 1 = 1

Usage :
	DECLARE @bcpQuery NVARCHAR(max)
	exec Staging.GetQueryWithHeader @tableName = 'MyTable' ,  @sql = @bcpQuery OUTPUT
	--PRINT @bcpQuery
  
  SET @bcpQuery = REPLACE('bcp.exe "{BCPQUERY}" queryout D:\tmp\example.txt -w -t ";" -S ServerName -U UserName -P Password', '{BCPQUERY}' , @bcpQuery)
	EXECUTE sp_executesql @bcpQuery
*/
CREATE PROCEDURE Staging.GetQueryWithHeader
	@schemaName	nvarchar(255) = 'dbo',
	@tableName		nvarchar(255),
	@filter			nvarchar(4000) = '',
	@columnsHeader	nvarchar(4000) = '',
	@columnsData	nvarchar(max) = '',
	@dateFormat		char(3) = '120',
	@sql			nvarchar(max) OUTPUT
AS
BEGIN
	IF @columnsHeader = '' AND @columnsData = ''
	BEGIN
		declare @columnsHeaderQuery nvarchar(4000)
		declare @columnsDataQuery nvarchar(4000)
		
		set @columnsHeaderQuery = 'set @columnsHeader = '''';	
			select @columnsHeader = @columnsHeader  + '''''''' + column_name + '''''','' 
			from INFORMATION_SCHEMA.COLUMNS 
			where TABLE_SCHEMA = ''' + @schemaName + ''' AND TABLE_NAME=''' + @tableName + ''''
		
		--cast given the column data type
		--date format can be specified
		set @columnsDataQuery = 'set @columnsData = '''';
			select @columnsData = @columnsData  + CASE DATA_TYPE	WHEN ''timestamp'' THEN ''sys.fn_varbintohexstr(''+column_name+'')''
			WHEN ''datetime'' THEN ''CONVERT(NVARCHAR(50) , ''+column_name+'', '+@dateFormat+')''
			WHEN ''smalldatetime'' THEN ''CONVERT(NVARCHAR(50) , ''+column_name+'', '+@dateFormat+')''
			ELSE ''CAST(''+column_name+'' AS NVARCHAR(4000))''
			END + '','' from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = ''' + @schemaName + ''' AND TABLE_NAME=''' + @tableName + ''''
			
		EXECUTE sp_executesql @columnsHeaderQuery , N'@columnsHeader nvarchar(4000) out' , @columnsHeader out
		EXECUTE sp_executesql @columnsDataQuery , N'@columnsData nvarchar(max) out' , @columnsData out
	
		--delete traling comma
		set @columnsHeader = SUBSTRING(@columnsHeader,0,LEN(@columnsHeader))
		set @columnsData = SUBSTRING(@columnsData,0,LEN(@columnsData))
	END
	--build the final request, @sql is the output parameter
	set @sql = 'SELECT ' + @columnsHeader + ' UNION ALL SELECT ' + @columnsData + ' FROM ' + @schemaName + '.' + @tableName + ' ' + @filter
END
