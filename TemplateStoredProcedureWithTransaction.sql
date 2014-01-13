/*
  Generic template for a stored procedure handling a transaction
*/
CREATE PROCEDURE [dbo].[MySP]
	@x INTEGER,
	@y INTEGER
AS
BEGIN
	SET XACT_ABORT, NOCOUNT ON
	
	BEGIN TRY
		BEGIN TRANSACTION
		-- TODO : Add your code here
		COMMIT TRANSACTION;
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
		BEGIN
			ROLLBACK TRANSACTION;
			PRINT 'ROLLBACKED !'
		END
		THROW; -- replace with a raiserror block if < SQL Server 2012
	END CATCH
END
