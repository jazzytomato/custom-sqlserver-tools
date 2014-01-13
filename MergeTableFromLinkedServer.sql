/*
TH 20131119
Procédure utilisée pour mettre à jour le réferentiel de développement depuis la recette.
Ajouter le serveur source dont les données sont à importer en serveur lié puis faire appel à cette procédure pour alimenter une table
On peux utiliser un serveur lié local 'self' lorsque les BDD sont sur le même serveur

Exemple :
exec Staging.MergeTableFromLinkedServer 'self' , 'MyBdd' , 'dbo' , 'Referentiel'
*/

CREATE PROCEDURE Staging.MergeTableFromLinkedServer
(	    @SourceLinkedServer NVARCHAR(255), --Serveur lié source
		@SourceDatabaseName NVARCHAR(255), -- BDD du serveur lié a utiliser
		@SchemaName NVARCHAR(255), --Schéma de la table source & cible
		@TableName NVARCHAR(255), -- Nom de la table source & cible
		/* facultatifs */
		@MergeKey NVARCHAR(MAX) = '', --Clé de mise à jour. PK par défaut, sinon mettre : source.colonne1 = target.colonne1 and ...
		@Filter NVARCHAR(MAX) = '' -- Filtre à utiliser pour la mise à jour, exemple : WHERE isDeleted = 0
)
AS
BEGIN
	BEGIN TRY
		--squelette du Merge
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

		--si il y a un identity dans la table, on spécifie identity insert à ON
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

		-- construction des listes de colonnes
		select	@ColumnList = @ColumnList  + '[' + column_name + '], '
				,@UpdateColumnList = @UpdateColumnList + CASE WHEN COLUMNPROPERTY(object_id(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 0 THEN '[' + column_name + '] = source.[' + column_name + '], ' ELSE '' END
				,@InsertColumnList = @InsertColumnList + 'source.[' + column_name + '], '
					from INFORMATION_SCHEMA.COLUMNS 
					where TABLE_SCHEMA = @SchemaName AND TABLE_NAME= @TableName



		select @ColumnList = SUBSTRING(@ColumnList,0,LEN(@ColumnList)) ,  @UpdateColumnList = SUBSTRING(@UpdateColumnList,0,LEN(@UpdateColumnList)) ,  @InsertColumnList = SUBSTRING(@InsertColumnList,0,LEN(@InsertColumnList)) 

		--Si la clé de mise à jour n'est pas spécifiée, on utilise la PK de la table
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

		
		--on insère dans la requête finale les paramétres fournis à la procédure
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

