#nullable enable
namespace MassTransit.EntityFrameworkCoreIntegration
{
    using Microsoft.EntityFrameworkCore;


    public interface ILockStatementProvider
    {
        string GetRowLockStatement<T>(DbContext context)
            where T : class;

        /// <summary>
        /// Returns the lock statement for the specified property (usable for any set)
        /// </summary>
        /// <param name="context"></param>
        /// <param name="propertyNames">One or more property names</param>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        string GetRowLockStatement<T>(DbContext context, params string[] propertyNames)
            where T : class;

        /// <summary>
        /// Returns the outbox query statement for the default bus (null BusName).
        /// </summary>
        string GetOutboxStatement(DbContext context);

        /// <summary>
        /// Returns the outbox query statement filtered by the specified bus name.
        /// </summary>
        /// <param name="context">The database context</param>
        /// <param name="busName">The bus name to filter by (null for the default IBus)</param>
        string GetOutboxStatement(DbContext context, string? busName);
    }
}
