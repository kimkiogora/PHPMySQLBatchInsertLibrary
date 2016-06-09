<?php
require "PHPMySQLBatchLibrary.php";

/**
 * @param $er
 * @throws Exception
 * Exception handler
 */
function exception_error_handler($errno, $errstr, $errfile, $errline)
{
    throw new ErrorException($errstr, $errno, 0, $errfile, $errline);
}

set_error_handler('exception_error_handler');
try {
   
    $columns = array(
        'columnA','columnB'//Place column IDs here
    );

    $table = 'TABLE_NAME';

    $credentials = array(
            'username'=>'username',
            'password'=>'password',
            'database'=>'database',
            'host'=>'server'
    );

    $batchManager = new PHPMySQLBatchLibrary();

    //Set the MySQL connection credentials
    $batchManager->setMySQLConnectionParameters($credentials);

    //Uncomment here if you want to inherit(use) Yii database parameters
    //$batchManager->setImportYiiMySQLConnectionParameters();

    //Set columns you want to insert values into
    $batchManager->setColumns($columns);

    //Set the table that you want to use
    $batchManager->setTable($table);

    $max = 100;
    $values= array();

     for($i=0;$i<$max;$i++){
        $z= $i+1;
        //If a value is unique use it in a similar way as $z above or insert queries will 
        //fail due to duplicate entry errors
        //If a value is a string use function; $batchManager->convertToMySQLString($param);
        $values[0] = VALUE_FOR_COLUMN_A;
        $values[1] = VALUE_FOR_COLUMN_B;
        $values[2] = VALUE_FOR_COLUMN_N;
    
        //add each of these values into a single batch
        $batchManager->_addBatchRecord($values);
    }
    //Process your transactions
    $results = $batchManager->processBatchTransactions();
    print_r($results);

} catch (Exception $e) {
    echo "\nAn error occurred ' " . $e->getMessage() . " ' LINE " . __LINE__ . "\n\n";
}
