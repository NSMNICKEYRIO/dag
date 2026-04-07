{
  "run_control_id": "Invoice_12",
  "triggerType": "O",
  "schedule": null,
  "nodes": [
    {
      "id": "JPMC_INVOICE_File_Transfer",
      "name": "JPMC_INVOICE_File_Transfer",
      "engine": "PYTHON",
      "executor_order_id": 1,
      "executor_sequence_id": 1,
      "execution_mode": "sync",
      "branch_on_status": false,
      "on_success_node_ids": [],
      "on_failure_node_ids": []
    },

    {
      "id": "JPMC_INVOICE_File_Parsing",
      "name": "JPMC_INVOICE_File_Parsing",
      "engine": "PYTHON",
      "executor_order_id": 2,
      "executor_sequence_id": 1,
      "execution_mode": "sync",
      "branch_on_status": false,
      "on_success_node_ids": [],
      "on_failure_node_ids": []
    },
    {
      "id": "JPMC_INVOICE_File_DB",
      "name": "JPMC_INVOICE_File_DB",
      "engine": "PYTHON",
      "executor_order_id": 2,
      "executor_sequence_id": 2,
      "execution_mode": "async_no_wait",
      "branch_on_status": false,
      "on_success_node_ids": [],
      "on_failure_node_ids": []
    },

    {
      "id": "JPMC_INVOICE_DB_FILE_1",
      "name": "JPMC_INVOICE_DB_FILE_1",
      "engine": "PYTHON",
      "executor_order_id": 3,
      "executor_sequence_id": 1,
      "execution_mode": "sync",
      "branch_on_status": false,
      "on_success_node_ids": [],
      "on_failure_node_ids": []
    },
    {
      "id": "JPMC_INVOICE_FEE_DB_FILE_2",
      "name": "JPMC_INVOICE_FEE_DB_FILE_2",
      "engine": "PYTHON",
      "executor_order_id": 3,
      "executor_sequence_id": 2,
      "execution_mode": "fire_and_forget",
      "branch_on_status": false,
      "on_success_node_ids": [],
      "on_failure_node_ids": []
    },

    {
      "id": "JPMC_INVOICE_FEE_DB_FILE_3",
      "name": "JPMC_INVOICE_FEE_DB_FILE_3",
      "engine": "PYTHON",
      "executor_order_id": 4,
      "executor_sequence_id": 99,
      "execution_mode": "sync",
      "branch_on_status": false,
      "on_success_node_ids": [],
      "on_failure_node_ids": []
    }
  ]
}
