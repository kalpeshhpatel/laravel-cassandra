<?php
/**
 * Created by PhpStorm.
 * User: realmile
 * Date: 14/2/14
 * Time: 1:05 PM
 */

namespace Kalpeshhpatel\LaravelCassandra;

use phpcassa\Connection\ConnectionPool;
use phpcassa\ColumnFamily;
use phpcassa\SystemManager;
use phpcassa\Schema\StrategyClass;


class Cassandra {

    private $connectionPool = null;

    private $keySpace = '';

    private $columnFamily = null;

    private $systemManager = null;

    /**
     * Make connection to connection pool
     */
    public function __construct() {
        try {
            $this->connect();
            $this->keySpace = \Config::get('laravel-cassandra::keyspace');
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * Function to deal with column family
     *  ie DML functions
     */

    /**
     * Close connection
     */
    public function __destruct() {
        try {
            $this->connectionPool->close();
            $this->systemManager->close();
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * Make connection to cluster and connection pool and return it
     * @return ConnectionPool
     */
    private function connect() {
        try {
            $this->systemManager = new SystemManager(\Config::get('laravel-cassandra::cassandra_node_ip'));
            $this->connectionPool = new ConnectionPool($this->keySpace, array(\Config::get('laravel-cassandra::cassandra_node_ip')));
            return;
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * Get object of requested column family
     * @param $columnFamily
     * @return ColumnFamily
     */
    public function getColumnFamily($columnFamily) {
        try {
            return $this->columnFamily = new ColumnFamily($this->connectionPool, $columnFamily);
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * @param $columnFamily
     * @param $key
     * @param null $columnSlice
     * @param null $columnNames
     * @param null $consistencyLevel
     * @param int $bufferSize
     * @return mixed
     */
    public function get($columnFamily,$key,$columnSlice=null,$columnNames=null,$consistencyLevel=null,$bufferSize=16) {
        try {
            $this->getColumnFamily($columnFamily);
            if(is_array($key))
                return $this->columnFamily->multiget($key,$columnSlice,$columnNames,$consistencyLevel,$bufferSize);
            else
                return $this->columnFamily->get($key,$columnSlice,$columnNames,$consistencyLevel);
        } catch (\Exception $e) {

        }
    }

    /**
     * Add data
     * @param $columnFamily
     * @param $key
     * @param $data
     * @param null $timeStamp
     * @param null $ttl
     * @param null $consistencyLevel
     * @return mixed
     */
    public function add($columnFamily,$key,$data,$timeStamp = null,$ttl = null, $consistencyLevel = null) {
        try {
            $this->getColumnFamily($columnFamily);
            return $this->columnFamily->insert($key,$data,$timeStamp,$ttl,$consistencyLevel);
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * To add multiple rows at a time
     * @param $columnFamily
     * @param $rows Array of rows to be inserted in format array(key => array(column_name => column_value))
     * @param null $timeStamp
     * @param null $ttl
     * @param null $consistencyLevel
     * @return mixed
     */
    public function multiAdd($columnFamily,$rows,$timeStamp = null,$ttl = null, $consistencyLevel = null) {
        try {
            $this->getColumnFamily($columnFamily);
            return $this->columnFamily->batch_insert($rows,$timeStamp,$ttl,$consistencyLevel);
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * Update data
     * @param $columnFamily
     * @param $key
     * @param $data
     * @param null $timeStamp
     * @param null $ttl
     * @param null $consistencyLevel
     */
    public function update($columnFamily,$key,$data,$timeStamp = null,$ttl = null, $consistencyLevel = null) {
        try {
            $this->add($columnFamily,$key,$data,$timeStamp,$ttl,$consistencyLevel);
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * Remove given row provide key
     * @param $columnFamily
     * @param $key
     * @param null $columnNames
     * @param null $consistencyLevel
     * @return mixed
     */
    public function remove($columnFamily,$key,$columnNames=null,$consistencyLevel=null) {
        try {
            $this->getColumnFamily($columnFamily);
            return $this->columnFamily->remove($key,$columnNames,$consistencyLevel);
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * Set TTL for given row
     * @param $columnFamily
     * @param $key
     * @param $ttl
     * @return mixed
     */
    public function setTtl($columnFamily,$key,$ttl) {
        try {
            $data = $this->get($columnFamily,$key);
            if(!empty($data)) {
                return $this->add($columnFamily,$key,$data,null,$ttl);
            }
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * This function is used to count how many columns are present in given row or given
     * rows depending upon if key is array or not
     * In case of multiple keys it returns array in format array('key'=>'column_count')
     * else it will return integer
     * @param $columnFamily
     * @param $key
     * @param null $columnSlice
     * @param null $columnNames
     * @param null $consistencyLevel
     * @return mixed
     */
    public function getColumnCount($columnFamily,$key,$columnSlice=null,$columnNames=null,$consistencyLevel=null) {
        try {
            $this->getColumnFamily($columnFamily);
            if(is_array($key))
                return $this->columnFamily->multiget_count($key,$columnSlice,$columnNames,$consistencyLevel);
            else
                return $this->columnFamily->get_count($key,$columnSlice,$columnNames,$consistencyLevel);
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * This function is used to truncate given columnfamily
     * @param $columnFamily
     */
    public function truncate($columnFamily) {
        try {
            $this->getColumnFamily(($columnFamily));
            $this->columnFamily->truncate();
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * Cassandra meta functions ie schema related functions
     */

    /**
     * This function is used to create column family
     * @param $keySpace
     * @param string $strategyClass
     * @param array $strategyOptions
     * @return mixed
     */
    public function createKeySpace($keySpace,$strategyClass = StrategyClass::SIMPLE_STRATEGY,$strategyOptions = array()) {
        try {
            if(empty($strategyOptions))
                $strategyOptions = array('replication_factor' => '1');
            return $this->systemManager->create_keyspace($keySpace,array('strategy_class'=>$strategyClass,'strategy_options'=>$strategyOptions));
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * Function to alter keySpace
     * @param $keySpace
     * @param $attributes
     * @return mixed
     */
    public function alterKeySpace($keySpace,$attributes) {
        try {
            return $this->systemManager->alter_keyspace($keySpace,$attributes);
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * Function to drop keySpace
     * @param $keySpace
     * @return mixed
     */
    public function dropKeySpace($keySpace) {
        try {
            return $this->systemManager->drop_keyspace($keySpace);
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * To create new column family
     * @param $keySpace
     * @param $columnFamily
     * @param $attributes array("column_type" => "Standard",
     *                          "comparator_type" => "org.apache.cassandra.db.marshal.AsciiType",
     *                          "memtable_throughput_in_mb" => 32);
     * @return mixed
     */
    public function createColumnFamily($keySpace,$columnFamily,$attributes = null) {
        try {
            return $this->systemManager->create_column_family($keySpace,$columnFamily,$attributes);
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * To alter column family
     * @param $keySpace
     * @param $columnFamily
     * @param null $attributes
     * @return mixed
     */
    public function alterColumnFamily($keySpace,$columnFamily,$attributes = null) {
        try {
            return $this->systemManager->alter_column_family($keySpace,$columnFamily,$attributes);
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * To drop column family
     * @param $keySpace
     * @param $columnFamily
     * @return mixed
     */
    public function dropColumnFamily($keySpace,$columnFamily) {
        try {
            return $this->systemManager->drop_column_family($keySpace,$columnFamily);
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * This function is used to truncate column family...
     * @param $keySpace
     * @param $columnFamily
     * @return mixed
     */
    public function truncateColumnFamily($keySpace,$columnFamily) {
        try {
            return $this->systemManager->truncate_column_family($keySpace,$columnFamily);
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * Adds an index to a column family.
     *
     * Example usage:
     *
     * <code>
     * $sys = new SystemManager();
     * $sys->create_index("Keyspace1", "Users", "name", "UTF8Type");
     * </code>
     *
     * @param $keySpace
     * @param $columnFamily
     * @param $column
     * @param null $dataType
     * @param null $indexName
     * @param null $indexType
     * @return mixed
     */
    public function createIndex($keySpace, $columnFamily, $column, $dataType= null, $indexName=NULL, $indexType=null) {
        try {
            return $this->systemManager->create_index(
                $keySpace,
                $columnFamily,
                $column,
                $dataType,
                $indexName,
                $indexType
            );
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * Drop an index from a column family.
     *
     * Example usage:
     *
     * <code>
     * $sys = new SystemManager();
     * $sys->drop_index("Keyspace1", "Users", "name");
     * </code>
     *
     *
     * @param $keySpace
     * @param $columnFamily
     * @param $column
     * @return mixed
     */
    public function dropIndex($keySpace, $columnFamily, $column) {
        try {
            return $this->systemManager->dropm_index($keySpace, $columnFamily, $column);
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * Changes or sets the validation class of a single column.
     *
     * Example usage:
     *
     * <code>
     * $sys = new SystemManager();
     * $sys->alter_column("Keyspace1", "Users", "name", "UTF8Type");
     * </code>
     *
     * @param $keySpace: the name of the keyspace containing the column family
     * @param $columnFamily: the name of the column family
     * @param $column
     * @param $dataType
     * @return mixed
     */
    public function alterColumn($keySpace, $columnFamily, $column, $dataType) {
        try {
            return $this->systemManager->alter_column($keySpace,$columnFamily,$column,$dataType);
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * Describes the Cassandra cluster.
     * @param $keySpace
     * @return mixed: The node to token mapping
     */
    public function describeRing($keySpace) {
        try {
            return $this->systemManager->describe_ring($keySpace);
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * Gives the cluster name.
     * @return mixed
     */
    public function describeClusterName() {
        try {
            return $this->systemManager->describe_cluster_name();
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * Gives the Thrift API version for the Cassandra instance.
     *
     * Note that this is different than the Cassandra version.
     *
     * @return string the API version
     */
    public function describeVersion() {
        try {
            return $this->systemManager->desctribe_version();
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * Describes what schema version each node currently has.
     * Differences in schema versions indicate a schema conflict.
     *
     * @return array a mapping of schema versions to nodes.
     */
    public function describeSchemaVersions() {
        try {
            return $this->systemManager->describe_schema_version();
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * Describes the cluster's partitioner.
     *
     * @return string the name of the partitioner in use
     */
    public function describePartitioner() {
        try {
            return $this->systemManager->describe_partitioner();
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * Describes the cluster's snitch.
     *
     * @return string the name of the snitch in use
     */
    public function describeSnitch() {
        try {
            return $this->systemManager->describe_snitch();
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * Returns a description of the keyspace and its column families.
     * This includes all configuration settings for the keyspace and
     * column families.
     *
     * @param string $keySpace the keyspace name
     *
     * @return mixed
     */
    public function describeKeySpace($keySpace) {
        try {
            return $this->systemManager->describe_keyspace($keySpace);
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

    /**
     * Like describeKeySpace(), but for all keyspaces.
     *
     * @return array an array of cassandra\KsDef
     */
    public function describeKeySpaces() {
        try {
            return $this->systemManager->describe_keyspaces();
        } catch (\Exception $e) {
            echo $e->getMessage();
        }
    }

}