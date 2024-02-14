<?php
namespace App\Models;


use CodeIgniter\Model;
use CodeIgniter\Database\ConnectionInterface;
use App\Models\ModelFieldType;

use App\Libraries\CoreEncryptor;

class DbManagerModel 
{
    protected $db, $table_name, $uri, $deleted_val, $encryptor;
    protected $_fieldTypes = [];



    public function __construct(ConnectionInterface &$db)
    {
        $this->db = &$db;

        $request = \Config\Services::request();

        $this->uri  = $request->getUri();
        
        $this->deleted_val = '0000-00-00 00:00:00';

        $this->encryptor = new CoreEncryptor();
    }

    public function tableSchema()
    {
        $database_name = $this->db->database;
        
        $builder = $this->db->table('information_schema.tables');
        // $builder->select('table_name');
        $builder->where('table_schema', $database_name); // 데이터베이스 이름으로 필터링합니다.
        $result = $builder->get();
        
        $tables = $result->getResult();

        return $tables;
    }

    public function schema($table)
    {
        $query = "SHOW COLUMNS FROM $table";
        $result = $this->db->query($query)->getResult();
        return $result;
    }

    public function insertItem($table, $data)
    {
        $this->db->table($table)->insert($data);
        return $this->db->insertID();
    }

    public function getAnyItems($table, $where = false)
    {
        $builder = $this->db->table($table);

        $builder->where(" (deleted_at = '{$this->deleted_val}' OR deleted_at IS NULL) ");

        if ($where)
            $builder->where($where);
            
        return $builder->get()->getResult();
    }
    
}