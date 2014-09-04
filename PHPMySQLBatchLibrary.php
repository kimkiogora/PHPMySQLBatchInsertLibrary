<?php
/**
 * Author: kim Kiogora
 * Date: 04/08/14
 * Time: 9:25 AM
 * Version: 1.0.1
 * Usage:
 *      This class is used for Batch MYSQL Inserts
 */
class PHPMySQLBatchLibrary
{

    /*--Variables--*/
    private $mysql_credentials;
    private $is_yii_powered;
    private $db_username;
    private $db_password;
    private $db_host;
    private $db_mydb;
    private $db_params;
    private $host_key_string = "mysql:host";
    private $db_key_string = "dbname";
    private $max_insert_limit;
    private $default_max_insert_limit = 1000;
    private $columns;
    private $columnValues = array();
    private $returnColumnValues;
    private $correspondingReturnColumnValues = array();
    private $table;
    private $TRANSACTION_SEQUENCE = "INSERT INTO";

    //--data structures--
    private $transactionsBatch = array();
    private $errors = array();

    /**
     * constructor
     */
    public function __construct()
    {
    }

    /**
     * @param $credentials
     * Set database credentials
     */
    public function setMySQLConnectionParameters($credentials)
    {
        $this->mysql_credentials = $credentials;

        if (empty($this->mysql_credentials)) {
            throw new Exception('MySQL Login credentials missing!');
        }
        $this->db_params = $this->mysql_credentials;
        $this->_setUsername();
        $this->_setPassword();
        $this->_setHost();
        $this->_setParentDatabase();
    }

    /**
     * Set connection parameters to that of Yii
     */
    public function setImportYiiMySQLConnectionParameters()
    {
        $this->is_yii_powered = TRUE;

        if (class_exists("Yii")) {
            $this->db_params = Yii::app()->db;
            $this->_setUsername();
            $this->_setPassword();
            $this->_setHost();
            $this->_setParentDatabase();
        } else {
            throw new Exception('Yii Class not found !');
        }
    }

    /**
     * Set the username
     */
    private function _setUsername()
    {
        $this->db_username = $this->db_params['username'];
        if(strcmp($this->db_username,"")==0 ){
            throw new Exception('Username must be specified !');
        }
    }

    /**
     * Set the password
     */
    private function _setPassword()
    {
        $this->db_password = $this->db_params['password'];
    }

    /**
     * Set the host
     */
    private function _setHost()
    {
        if ($this->is_yii_powered == TRUE) {
            $host = $this->db_params['connectionString'];
            $joinedParams = explode(";", $host);
            $this->db_host = str_replace($this->host_key_string . "=", "", $joinedParams[0]);
        } else {
            if (isset($this->db_params['host'])) {
                $this->db_host = $this->db_params['host'];
            } else {
                throw new Exception('Database Host/Server not provided !');
            }
        }
    }

    /**
     * Set the database
     */
    private function _setParentDatabase()
    {
        if ($this->is_yii_powered == TRUE) {
            $mydb = $this->db_params['connectionString'];
            $joinedParams = explode(";", $mydb);
            $this->db_mydb = str_replace($this->db_key_string . "=", "", $joinedParams[1]);
        } else {
            if (isset($this->db_params['database'])) {
                $this->db_mydb = $this->db_params['database'];
            } else {
                throw new Exception('Database not provided !');
            }
        }
    }

    /**
     * @param $columns
     * Set the columns to insert data into
     */
    public function setColumns($columns)
    {
        $this->columns = $columns;
        if(count($this->columns) <=0){
            throw new Exception('Column(s) not provided !');
        }
    }

    /**
     * @param $columnsToReturn
     * Set the columns that you need a return value from
     * e.g if inserting a transaction in a transaction
     * table, return the transactionID e.t.c
     */
    public function setReturnColumns($columnValuesToReturn)
    {
        $this->returnColumnValues = $columnValuesToReturn;
    }

    /**
     * Set the columnIDs corresponding to the valuesID
     * This will always match with the values provided
     */
    private function _setCorrespondingColumnIDs($provided_columns, $returnColumns, $stack_of_records)
    {
        $n = 0;
        for ($i = 0; $i < count($returnColumns); $i++) {
            $values = array();
            for ($j = 0; $j < count($provided_columns); $j++) {
                if ($returnColumns[$i] == $provided_columns[$j]) {
                    $column = $this->columns[$j];
                    for ($k = 0; $k < count($stack_of_records); $k++) {
                        $values[$n] = $stack_of_records[$k][$j];
                        $n++;
                    }
                    $this->correspondingReturnColumnValues[$column] = $values;
                }
            }
            $n = 0;
        }
    }

    /**
     * @param $tableIs
     * Set the table to work with i.e insert data
     */
    public function setTable($tableIs)
    {
        $this->table = $tableIs;
        if(empty($this->table)){
            throw new Exception('Table not provided !');
        }
    }

    /**
     * @param $mini_batch
     * Set a mini-batch of transactions e.g If you have a lot of transactions that may
     * consume
     */
    public function setBatchLimit($mini_batch)
    {
        if ($mini_batch == NULL or $mini_batch <= 0) {
            $this->max_insert_limit = $mini_batch;
        } else {
            $this->max_insert_limit = $this->default_max_insert_limit;
        }
    }

    /**
     * @param $table
     * @param $options
     * @throws Exception
     */
    public function _addBatchRecord($options)
    {
        array_push($this->columnValues, $options);

        $transactions_batch_query_string = $this->TRANSACTION_SEQUENCE." ".$this->table." (";

        $total_columns = count($this->columns);
        $total_values = count($options);

        if ($total_columns != $total_values) {
            throw new Exception('Value count does not match column count!');
        }

        $columnString = null;
        for ($i = 0; $i < $total_columns; $i++) {
            if ($columnString != null) {
                $columnString .= "," . $this->columns[$i];
            } else {
                $columnString .= $this->columns[$i];
            }
        }
        $columnString .= ") VALUES (";
        $transactions_batch_query_string .= $columnString;
        $valueString = null;

        for ($j = 0; $j < $total_values; $j++) {
            if ($valueString != null) {
                $valueString .= "," . $options[$j];
            } else {
                $valueString .= $options[$j];
            }
        }
        $valueString .= ")";
        $transactions_batch_query_string .= $valueString;
        array_push($this->transactionsBatch, $transactions_batch_query_string);
    }


    /**
     * Process transactions
     */
    public function processBatchTransactions()
    {
        global $status;
        $status = FALSE;
        $results = array();
        $queryString = null;

        //set column values
        $this->_setCorrespondingColumnIDs($this->columns, $this->returnColumnValues, $this->columnValues);

        //set time
        $count = count($this->transactionsBatch);
        $link = mysqli_connect($this->db_host, $this->db_username, $this->db_password, $this->db_mydb);

        // check connection
        if (mysqli_connect_errno()) {
            array_push($this->errors, mysqli_connect_error());
            return NULL;
        }

        //Ensure we have a mini batch to reduce resource consumption
        if ($this->max_insert_limit == NULL or $this->max_insert_limit <= 0) {
            $this->max_insert_limit = $this->default_max_insert_limit;
        }

        if ($count < $this->max_insert_limit) {
            $results = $this->push($link, $queryString, $this->transactionsBatch);
        } else {
            $batch_counter = 0;
            $counter = 0;
            while ($counter < $count) {
                $data = array_slice($this->transactionsBatch, $batch_counter, $this->max_insert_limit);
                if (empty($data)) {
                    break;
                }
                $results = $this->push($link, $queryString, $data);
                $batch_counter += $this->max_insert_limit;
                $counter++;
            }
        }
        return $results;
    }

    /**
     * @param $count
     * @param $transactions
     */
    private function push($link, $queryString, $transactionsBatch)
    {
        for ($i = 0; $i < count($transactionsBatch); $i++) {
            $queryString .= $transactionsBatch[$i];
        }
        $queryString = implode(";", $transactionsBatch);
        if (mysqli_multi_query($link, $queryString)) {
            $i = 0;
            do {
                $i++;
                if (!mysqli_more_results($link)) {
                    break;
                }
            } while (mysqli_next_result($link));

            $status = "SUCCESS";
            $results ["Status"] = $status;
        } else {
            $status = "FAILED";
            $results ["Status"] = $status;
            $results ["Error"] = mysqli_error($link);
        }
        return $results;
    }

    /**
     * Retrieve the list of inserted records safely
     */
    public function getListOfInsertedRecords()
    {
        $list = array();
        if ($this->correspondingReturnColumnValues != NULL) {
            return $this->correspondingReturnColumnValues;
        }
        return $list;
    }

    /**
     * @param $param
     * Return a MySQL compatible parameter such as numbers
     */
    public function convertToMySQLString($param)
    {
        if ($param != null) {
            return "'$param'";
        }
        return NULL;
    }
}
