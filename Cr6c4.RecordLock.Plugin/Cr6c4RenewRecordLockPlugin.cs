using System;
using System.ServiceModel;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;

namespace Cr6c4.RecordLock.Plugin
{
    public class Cr6c4RenewRecordLockPlugin : IPlugin
    {
        private const string RecordLockTable = "cr6c4_record_lock";

        public void Execute(IServiceProvider serviceProvider)
        {
            var context = (IPluginExecutionContext)serviceProvider.GetService(typeof(IPluginExecutionContext));
            var factory = (IOrganizationServiceFactory)serviceProvider.GetService(typeof(IOrganizationServiceFactory));
            var service = factory.CreateOrganizationService(context.UserId);

            // ===== Input =====
            var targetTable = (string)context.InputParameters["TargetTableLogicalName"];
            var targetRecordId = (string)context.InputParameters["TargetRecordId"];
            var sessionId = (string)context.InputParameters["ClientSessionId"];
            var extendSeconds = (int)context.InputParameters["ExtendSeconds"];

            // ★ Power Apps が保持する「ロック行の versionnumber」を受け取る（文字列として渡す）
            var expectedRowVersion = (string)context.InputParameters["ExpectedRowVersion"];

            var nowUtc = DateTime.UtcNow;

            try
            {
                // 1) 対象ロック行を取得（セッション＆期限チェック用）
                var qe = new QueryExpression(RecordLockTable)
                {
                    ColumnSet = new ColumnSet("cr6c4_client_session_id", "cr6c4_expires_at"),
                    TopCount = 1
                };
                qe.Criteria.AddCondition("cr6c4_target_table_logical_name", ConditionOperator.Equal, targetTable);
                qe.Criteria.AddCondition("cr6c4_target_record_id", ConditionOperator.Equal, targetRecordId);

                var rows = service.RetrieveMultiple(qe);
                if (rows.Entities.Count == 0)
                {
                    context.OutputParameters["Result"] = "LOST";
                    context.OutputParameters["ServerTimeUtc"] = nowUtc;
                    context.OutputParameters["Message"] = "Lock row not found.";
                    return;
                }

                var lockRow = rows.Entities[0];

                // 2) 所有者＆期限チェック
                var rowSessionId = lockRow.GetAttributeValue<string>("cr6c4_client_session_id");
                var expiresAtUtc = lockRow.GetAttributeValue<DateTime?>("cr6c4_expires_at");

                var valid =
                    string.Equals(rowSessionId, sessionId, StringComparison.OrdinalIgnoreCase) &&
                    (!expiresAtUtc.HasValue || expiresAtUtc.Value > nowUtc);

                if (!valid)
                {
                    context.OutputParameters["Result"] = "LOST";
                    context.OutputParameters["ServerTimeUtc"] = nowUtc;
                    context.OutputParameters["Message"] =
                        "Lock is not owned by this session or already expired.";
                    return;
                }

                // 3) 更新値（延長）
                var newExpiresAtUtc = nowUtc.AddSeconds(extendSeconds);

                // 4) ★ RowVersion 一致時のみ更新（= DB側で versionnumber チェック）
                // UpdateRequest + ConcurrencyBehavior.IfRowVersionMatches が公式のやり方[1](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/optimistic-concurrency)[2](https://github.com/microsoft/PowerApps-Samples/blob/master/dataverse/orgsvc/C%23/OptimisticConcurrency/OptimisticConcurrency/SampleProgram.cs)
                var upd = new Entity(RecordLockTable, lockRow.Id)
                {
                    RowVersion = expectedRowVersion
                };
                upd["cr6c4_locked_at"] = nowUtc;
                upd["cr6c4_expires_at"] = newExpiresAtUtc;
                upd["cr6c4_client_session_id"] = sessionId;

                var req = new UpdateRequest
                {
                    Target = upd,
                    ConcurrencyBehavior = ConcurrencyBehavior.IfRowVersionMatches
                };

                service.Execute(req);

                // 5) 成功後：最新RowVersionを取り直して返す（次回延長用）
                // サンプルでも更新後に再RetrieveしてRowVersionを確認する流れが一般的[2](https://github.com/microsoft/PowerApps-Samples/blob/master/dataverse/orgsvc/C%23/OptimisticConcurrency/OptimisticConcurrency/SampleProgram.cs)
                var refreshed = service.Retrieve(RecordLockTable, lockRow.Id, new ColumnSet());
                var newRowVersion = refreshed.RowVersion;

                context.OutputParameters["Result"] = "OK";
                context.OutputParameters["ServerTimeUtc"] = nowUtc;
                context.OutputParameters["NewExpiresAtUtc"] = newExpiresAtUtc;
                context.OutputParameters["NewRowVersion"] = newRowVersion;
                context.OutputParameters["Message"] = "Renewed.";
            }
            catch (FaultException<OrganizationServiceFault> fault)
            {
                // RowVersion 不一致など（ConcurrencyVersionMismatch等）で更新が拒否され得る[1](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/optimistic-concurrency)
                context.OutputParameters["Result"] = "CONFLICT";
                context.OutputParameters["ServerTimeUtc"] = nowUtc;
                context.OutputParameters["Message"] = fault.Detail?.Message ?? fault.ToString();
            }
            catch (Exception ex)
            {
                context.OutputParameters["Result"] = "ERROR";
                context.OutputParameters["ServerTimeUtc"] = nowUtc;
                context.OutputParameters["Message"] = ex.ToString();
            }
        }
    }
}