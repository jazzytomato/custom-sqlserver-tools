/*
Stored procedure that automatically merge a table from another table. The two tables structures needs to be identical.
It is useful to update referentials through different environements (i.e. from production to developement).

Usage:
	- Add source server as a linked server (if the DB are on the same server, use a local loopback)
	- exec Staging.MergeTableFromLinkedServer 'self' , 'MyBdd' , 'dbo' , 'MyReferential'

*/
CREATE PROCEDURE Staging.MergeTableFromLinkedServer
(	    @SourceLinkedServer NVARCHAR(255), -- source linked server
		@SourceDatabaseName NVARCHAR(255), -- source database on the linked server
		@SchemaName NVARCHAR(255), -- schema name (both source & target)
		@TableName NVARCHAR(255), -- table name (both source & target)
		/* optional */
		@MergeKey NVARCHAR(MAX) = '', -- Update key (default is the primary key of the table). Otherwise, use  : source.c1 = target.c1 and ...
		@Filter NVARCHAR(MAX) = '' -- example : WHERE isDeleted = 0
)
AS
BEGIN
	BEGIN TRY
		--Merge skeleton
		DECLARE @sqlMerge NVARCHAR(MAX) =
		'MERGE $(SchemaName).$(TableName) AS target
			USING (
				SELECT 
					$(ColumnList)
				FROM OPENQUERY($(SourceLinkedServer),''SELECT $(ColumnList) FROM $(SourceDatabaseName).$(SchemaName).$(TableName) $(Filter)'')
				) AS source
				ON ($(MergeKey))
			WHEN MATCHED
				THEN
					UPDATE
					SET 
					 $(UpdateColumnList)
			WHEN NOT MATCHED
				THEN
					INSERT (
					$(ColumnList)
					)
					VALUES (
					$(InsertColumnList)
					); ';

		--if the table contains and identity column, specify identity insert ON
		IF EXISTS (
			select 1 from sys.objects o 
			inner join sys.columns c on o.object_id = c.object_id
			inner join sys.schemas s on s.schema_id = o.schema_id
			where c.is_identity = 1 and s.name = @SchemaName and o.name = @TableName
		)
			SET @sqlMerge = 'SET IDENTITY_INSERT $(SchemaName).$(TableName) ON; ' + @sqlMerge + ' SET IDENTITY_INSERT $(SchemaName).$(TableName) OFF;';

		
		DECLARE @ColumnList NVARCHAR(MAX) = '' , 
				@UpdateColumnList NVARCHAR(MAX) = '',
				@InsertColumnList NVARCHAR(MAX) = ''

		-- build column list
		select	@ColumnList = @ColumnList  + '[' + column_name + '], '
				,@UpdateColumnList = @UpdateColumnList + CASE WHEN COLUMNPROPERTY(object_id(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 0 THEN '[' + column_name + '] = source.[' + column_name + '], ' ELSE '' END
				,@InsertColumnList = @InsertColumnList + 'source.[' + column_name + '], '
					from INFORMATION_SCHEMA.COLUMNS 
					where TABLE_SCHEMA = @SchemaName AND TABLE_NAME= @TableName



		select @ColumnList = SUBSTRING(@ColumnList,0,LEN(@ColumnList)) ,  @UpdateColumnList = SUBSTRING(@UpdateColumnList,0,LEN(@UpdateColumnList)) ,  @InsertColumnList = SUBSTRING(@InsertColumnList,0,LEN(@InsertColumnList)) 

		--If the update key isn't specified, use the PK
		IF @MergeKey = ''
		BEGIN
			select @MergeKey = @MergeKey  + 'source.[' + kcu.COLUMN_NAME + '] = target.[' + kcu.COLUMN_NAME  + '] AND '
			  from INFORMATION_SCHEMA.TABLE_CONSTRAINTS as tc
			  join INFORMATION_SCHEMA.KEY_COLUMN_USAGE as kcu
				on kcu.CONSTRAINT_SCHEMA = tc.CONSTRAINT_SCHEMA
			   and kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
			   and kcu.TABLE_SCHEMA = tc.TABLE_SCHEMA
			   and kcu.TABLE_NAME = tc.TABLE_NAME
			 where tc.CONSTRAINT_TYPE = 'PRIMARY KEY' and kcu.TABLE_SCHEMA = @SchemaName AND kcu.TABLE_NAME = @TableName
			 order by kcu.ORDINAL_POSITION
 
			SELECT @MergeKey = SUBSTRING(@MergeKey,0,LEN(@MergeKey) - 3) --supprime le trailing 'AND'
		END

		
		--replace the parameters into the final request
		SET @sqlMerge = REPLACE(@sqlMerge,'$(ColumnList)',@ColumnList)
		SET @sqlMerge = REPLACE(@sqlMerge,'$(UpdateColumnList)',@UpdateColumnList)
		SET @sqlMerge = REPLACE(@sqlMerge,'$(InsertColumnList)',@InsertColumnList)

		SET @sqlMerge = REPLACE(@sqlMerge,'$(SourceLinkedServer)',@SourceLinkedServer)
		SET @sqlMerge = REPLACE(@sqlMerge,'$(SourceDatabaseName)',@SourceDatabaseName)
		SET @sqlMerge = REPLACE(@sqlMerge,'$(SchemaName)',@SchemaName)
		SET @sqlMerge = REPLACE(@sqlMerge,'$(TableName)',@TableName)
		SET @sqlMerge = REPLACE(@sqlMerge,'$(MergeKey)',@MergeKey)
		SET @sqlMerge = REPLACE(@sqlMerge,'$(Filter)',@Filter)

		PRINT @sqlMerge;
		EXECUTE sp_executesql @sqlMerge

	END TRY
	BEGIN CATCH
		THROW;
	END CATCH
END

