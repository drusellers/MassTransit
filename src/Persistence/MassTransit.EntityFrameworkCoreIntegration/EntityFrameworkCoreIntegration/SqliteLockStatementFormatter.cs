#nullable enable
namespace MassTransit.EntityFrameworkCoreIntegration
{
    using System.Text;


    public class SqliteLockStatementFormatter :
        ILockStatementFormatter
    {
        public void Create(StringBuilder sb, string schema, string table)
        {
            sb.Append($@"SELECT * FROM ""{table}"" WHERE ");
        }

        public void AppendColumn(StringBuilder sb, int index, string columnName)
        {
            sb.Append(index == 0 ? $@"""{columnName}"" = @p0" : $@" AND ""{columnName}"" = @p{index}");
        }

        public void Complete(StringBuilder sb)
        {
        }

        public void CreateOutboxStatement(StringBuilder sb, string schema, string table, string columnName)
        {
            sb.Append($@"SELECT * FROM ""{table}"" ORDER BY ""{columnName}"" LIMIT 1");
        }

        public void CreateOutboxStatement(StringBuilder sb, string schema, string table, string createdColumnName, string busNameColumnName, string? busName)
        {
            if (busName == null)
            {
                sb.Append($@"SELECT * FROM ""{table}"" WHERE ""{busNameColumnName}"" IS NULL ORDER BY ""{createdColumnName}"" LIMIT 1");
            }
            else
            {
                sb.Append($@"SELECT * FROM ""{table}"" WHERE ""{busNameColumnName}"" = '{busName}' ORDER BY ""{createdColumnName}"" LIMIT 1");
            }
        }
    }
}
