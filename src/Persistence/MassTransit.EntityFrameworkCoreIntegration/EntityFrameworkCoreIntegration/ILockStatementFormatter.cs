#nullable enable
namespace MassTransit.EntityFrameworkCoreIntegration
{
    using System.Text;


    public interface ILockStatementFormatter
    {
        void Create(StringBuilder sb, string schema, string table);
        void AppendColumn(StringBuilder sb, int index, string columnName);
        void Complete(StringBuilder sb);

        void CreateOutboxStatement(StringBuilder sb, string schema, string table, string columnName);

        /// <summary>
        /// Creates an outbox statement filtered by bus name.
        /// </summary>
        /// <param name="sb">The string builder</param>
        /// <param name="schema">The database schema</param>
        /// <param name="table">The table name</param>
        /// <param name="createdColumnName">The Created column name</param>
        /// <param name="busNameColumnName">The BusName column name</param>
        /// <param name="busName">The bus name to filter by (null for the default IBus)</param>
        void CreateOutboxStatement(StringBuilder sb, string schema, string table, string createdColumnName, string busNameColumnName, string? busName);
    }
}
