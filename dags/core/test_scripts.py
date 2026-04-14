from .extract.data_checker import DataChecker

checker = DataChecker(
    table_name="sales",
    retail_conn_id="test_bd_reatil",
    mode="consistency",           # или "consistency"
    key_column="document_uid"
)

uids = checker.detect_changes()

print("Первые 5 UID:")
print(uids[:5])
