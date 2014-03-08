CREATE FUNCTION escapeXml 
(@xml nvarchar(MAX))
RETURNS nvarchar(MAX)
AS
BEGIN
    declare @return nvarchar(MAX)
    select @return = 
    REPLACE(
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(@xml,'&', '&amp;')
                ,'<', '&lt;')
            ,'>', '&gt;')
        ,'"', '&quot;')
    ,'''', '&#39;')

return @return
end
GO
