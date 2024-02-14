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


    public function updateItem($table, $where, $data)
    {
        $this->db->table($table)->where($where)->update($data);
        return $this->db->affectedRows();
    }

    function get_primary_key_field_name($table)
    {
        $query = "SHOW KEYS FROM $table WHERE Key_name = 'PRIMARY'";
        // TODO PK 지정되지 않은 테이븛의 경우 오류
        return $this->db->query($query)->getRow()->Column_name;
    }

    //Get one item
    public function getItem($table, $where)
    {
        $query = $this->db->table($table);

        $query->where(" (deleted_at = '{$this->deleted_val}' OR deleted_at IS NULL) ");

        if (!empty($where)) {
            $query->where($where);
        }
    
        $result = $query->get()->getRow();

        if ($result === null) {

            // 오류 처리

        }

        // echo $this->db->getLastQuery();
        // exit;

        return $result;
    }

    public function getAllItem($table, $where)
    {
        $query = $this->db->table($table);

        if (!empty($where)) {
            $query->where($where);
        }
    
        $result = $query->get()->getRow();

        if ($result === null) {

            // 오류 처리

        }

        // echo $this->db->getLastQuery();
        // exit;

        return $result;
    }

    public function getAllItems($table, $where = null)
    {
        $builder = $this->db->table($table);

        if ($where)
            $builder->where($where);

        $items = $builder->get()
            ->getResult();

        return $items;
    }

    function getCountTotal($table, $where = null){
   
        $count_query = "SELECT COUNT(*) as total FROM " . $table;

        $count_query .= " WHERE (".$table.".deleted_at = '". $this->deleted_val ."' OR ".$table.".deleted_at IS NULL) ";

        if ($where) {
            $count_query .= " AND (";
            $i = 0;
            foreach ($where as $key => $value) {
                if ($i > 0)
                    $count_query .= " AND ";

                //Check if operator is different from = (equal sign)
                $operator_arr = explode(" ", $key);
                if (isset($operator_arr[1]))
                    $operator = $operator_arr[1];
                else
                    $operator = "=";

                $count_query .= " `$operator_arr[0]`$operator" . $this->db->escape($value) . " ";
                $i++;
            }
            $count_query .= ")";
        }


        return $this->db->query($count_query)->getRow()->total;
    }


    function getCountGroup($table, $where = null, $group = null, $order = null, $limit = null){
   
        $count_query = "SELECT COUNT(*) as total, ".$group." FROM " . $table;

        $count_query .= " WHERE (".$table.".deleted_at = '". $this->deleted_val ."' OR ".$table.".deleted_at IS NULL) ";

        if ($where) {
            $count_query .= " AND (";
            $i = 0;
            foreach ($where as $key => $value) {
                if ($i > 0)
                    $count_query .= " AND ";

                //Check if operator is different from = (equal sign)
                $operator_arr = explode(" ", $key);
                if (isset($operator_arr[1]))
                    $operator = $operator_arr[1];
                else
                    $operator = "=";

                $count_query .= " `$operator_arr[0]`$operator" . $this->db->escape($value) . " ";
                $i++;
            }
            $count_query .= ")";
        }

        if ($group) {

            $count_query .= " GROUP BY ".$group." ";

        }

        if ($order) {
            $count_query .= " ORDER BY ";
            $count_query .= $order[0] . " " . $order[1];    
        }

        if ($limit) {
            $count_query .= " LIMIT 0, $limit ";
        }

        return $this->db->query($count_query)->getResultArray();
    }
    //Get total rows per request
    public function countTotalRows($table, $where = null, $request, $schema, $fields)
    {
        

        $count_query = "SELECT COUNT(*) as total FROM " . $table;
        //get primary_key field name
        //지점의 경우 Session에 있는 pt_code로 partner_branch 테이블 선택
        $pt_code = session()->get('pt_code');
        
        // TODO relation table 사용시 pk가 중복되어 pk_id로 구분.
        $pk = $this->get_primary_key_field_name($table);

        $is_search = $request->getPost('is_search');
        
        $keys_arr = array();

        
        if ($fields) {
            foreach ($fields as $key => $rel_field) {
                $keys_arr[] = $key;
            }
        }

        $result_query_rel = '';

        $rel_select_arr = array();

        if ($fields) {
        //Check for relation fields
            foreach ($fields as $key => $rel_field) {

                if (isset($rel_field['relation']) && !isset($rel_field['relation']['save_table'])) {
                    $rfield = $rel_field['relation'];

                    //지점의 경우 Session에 있는 pt_code로 partner_branch 테이블 선택
                    if ($pt_code && $rfield['table'] == 'partner_branch' || $pt_code && $rfield['table'] == 'sales_record') {
                        $rtable = $pt_code.'_'.$rfield['table'];
                        $rtable_name = $pt_code.'_'.$rfield['table'].'_'.$key;
                    }else{
                        $rtable = $rfield['table'];
                        $rtable_name = $rfield['table'].'_'.$key;
                    }

                    $count_query .= " LEFT JOIN  " . $rtable . " as ".$rtable_name." ON " . $table . '.' . $key . "=" . $rtable_name . "." . $rfield['primary_key'] . "  ";

                }
            }

        }
        
        if ($where) {
            $count_query .= " WHERE (".$table.".deleted_at = '". $this->deleted_val ."' OR ".$table.".deleted_at IS NULL) AND (";
            $i = 0;
            foreach ($where as $key => $value) {
                if ($i > 0)
                    $count_query .= " AND ";

                //Check if operator is different from = (equal sign)
                $operator_arr = explode(" ", $key);
                if (isset($operator_arr[1]))
                    $operator = $operator_arr[1];
                else
                    $operator = "=";

                if($is_search){
                    $operator = " LIKE ";
                }

                if (is_array($value)) {
                    // TODO 데이터셋에서 조건을 걸 때 키 값과 조건이 동일할 경우 배열로 처리
                    // 배열인 경우 분기처리
                    $j = 0;

                    foreach ($value as $op_key => $op_value) {
                        if ($j > 0)
                            $count_query .= " AND ";

                        $count_query .= $table.".".$operator_arr[0].$operator."'".$op_value."'";

                        $j++;
                    }

                }else{

                    $count_query .= $table.".".$operator_arr[0].$operator."'".$value."'";

                }



                //$this->db->where($key, $value);
                $i++;
            }
            $count_query .= ")";
        }else{
            $count_query .= " WHERE (".$table.".deleted_at = '". $this->deleted_val ."' OR ".$table.".deleted_at IS NULL) ";
        }


        if ($request->getPost('table_search')) {

            $allEmpty = true;
            $tempQuery = ' AND '; //deleted_at 쿼리
            $i = 0;
            // if ($where)
            //     $tempQuery .= " AND ";
            // else
            //     $tempQuery .= " WHERE ";

            //$tempQuery .= " ( ";

            foreach ($schema as $column) {

                if ($request->getPost($column->Field) == '')
                    continue;

                $allEmpty = false;
                $col_search = [];
                $col_search[] = $column->Field;

                if (isset($fields[$column->Field]['relation']) && isset($fields[$column->Field]['relation']['save_table'])) {




                    //Search relational table to get the ids of related ids
                    $relField = $fields[$column->Field]['relation'];

                    $parent_table = $relField['table'];

                    $relation_table = $relField['save_table'];
                    $joinString = $relation_table . '.' . $relField['child_field'] . '=' . $parent_table . '.' . $relField['primary_key'];
                    $likeColumns = $relField['display'];
                    $likeTerm = $request->getPost($column->Field);
                    //$relselect is optional. when used it will add DISTINCT to prevent dublicates
                    $relSelect = $relation_table . '.' . $relField['parent_field'];

                    if (isset($fields[$column->Field]['filter']) && $fields[$column->Field]['filter'] && $fields[$column->Field]['type'] == 'multiselect') {
                        $relatedItems = $this->filterRelatedItems($parent_table, $relation_table, $joinString, $likeColumns, $likeTerm, $this->uri->getSegment(2), $column->Field, $relSelect);
                    }else{
                        $relatedItems = $this->searchRelatedItems($parent_table, $relation_table, $joinString, $likeColumns, $likeTerm, $relSelect);
                    }

                    $relatedItemsIdArr = [];
                    if (!$relatedItems)
                        $relatedItemsIdArr = '-1';
                    else {
                        //Create an array of ids for whereIn statement
                        foreach ($relatedItems as $relatedItem) {
                            $relatedItemsIdArr[] = $relatedItem->{$relField['parent_field']};
                        }
                    }

                    if ($i > 0)
                        $tempQuery .= " AND ";

                    if (is_array($relatedItemsIdArr))
                        $relTempQuery = '' . $table . '.' . $pk . ' IN (' . implode(',', $relatedItemsIdArr) . ')';
                    else
                        $relTempQuery = $table . '.' . $pk . ' = ' . $relatedItemsIdArr;

                    //  echo $tempQuery.'<br>';
                    $tempQuery .= $relTempQuery;
                    //  echo $tempQuery;

                    $i++;
                    //$allEmpty = false;
                    continue;
                } else if (isset($fields[$column->Field]['relation'])) {

                    $col_search = $fields[$column->Field]['relation']['display'];
                    //check if display is an array of columns
                    if (!is_array($col_search))
                        $col_search[] = $col_search;

                    $table_search      = $fields[$column->Field]['relation']['table'];

                    $table_search_name = $table_search.'_'.$column->Field;

                } else if ($column->Type == 'datetime') {

                    $table_search = $table;
                    $table_search_name = $table;

                }else if (isset($fields[$column->Field]['callback']) && ($fields[$column->Field]['callback'] == 'callback_DecryptToCheck' || $fields[$column->Field]['callback'] == 'callback_sms_modal')){

                    $col_search = [];

                }else {
                    $table_search = $table;
                    $table_search_name = $table;

                    //$col_search[] = $column->Field;
                }




                if ($i > 0)
                    $tempQuery .= " AND ";





                if ($column->Type == 'datetime') {


                    $fr_date = $request->getPost('fr_date');
                    $to_date = $request->getPost('to_date');

                    if ($fr_date || $to_date) {

                        $searchLikeTempQuery = '';
                        $searchLikeTempQueryArr = [];
                        $searchLikeTempQueryArr[] = $this->generateDateClause($table_search, $column->Field, $fr_date, $to_date);
                    }




                }else{

                    //For loop is required when search must be performed in multiple relational columns from another table
                    $searchLikeTempQuery = '';
                    $searchLikeTempQueryArr = [];
                    foreach ($col_search as $colToSearch) {

                        if (isset($fields[$column->Field]['filter']) && $fields[$column->Field]['filter']){

                            $searchLikeTempQueryArr[] = $this->generateEqualClause($table_search, $table_search_name, $colToSearch, $request->getPost($column->Field));

                        }else{
                            $searchLikeTempQueryArr[] = $this->generateLikeClause($table_search, $table_search_name, $colToSearch, $request->getPost($column->Field));
                        }

                    }

                }

                if (isset($fields[$column->Field]['callback']) && ($fields[$column->Field]['callback'] == 'callback_DecryptToCheck' || $fields[$column->Field]['callback'] == 'callback_sms_modal')) {
                    $table_search = $table;  
                    $table_search_name = $table;  
                    $table_search_field = $this->encryptor->EncryptToCheck($request->getPost($column->Field));
                    
                    $searchLikeTempQueryArr[] = $this->generateEqualClause($table_search, $table_search_name, $column->Field, $table_search_field);      

                }

                if (count($col_search) > 1) {
                    $searchLikeTempQuery = implode(' OR ', $searchLikeTempQueryArr);
                    $searchLikeTempQuery = "($searchLikeTempQuery)";
                } else
                    $searchLikeTempQuery = $searchLikeTempQueryArr[0];


                $tempQuery .= $searchLikeTempQuery;

                //$this->db->like($table_search.'.'.$col_search, $request->getPost($column->Field), 'both');
                $i++;
            }

            $tempQuery .= isset($searchLikeTempQuery) ?  "  " : "";

            if (!$allEmpty)
                $count_query .= $tempQuery;
        }

        return $this->db->query($count_query)->getRow()->total;
    }



    public function getItems($table, $where = null, $request, $schema, $fields, $order, $offset, $per_page)
    {
        //지점의 경우 Session에 있는 pt_code로 partner_branch 테이블 선택
        $pt_code = session()->get('pt_code');

        // TODO relation table 사용시 pk가 중복되어 pk_id로 구분.
        $pk = $this->get_primary_key_field_name($table);

        
        //get primary_key field name
        $result_query = "SELECT *, ".$table.".".$pk." as pk_id FROM " . $table;  
        
        $is_search = $request->getPost('is_search');

        $keys_arr = array();

        foreach ($fields as $key => $rel_field) {
            $keys_arr[] = $key;
        }


        $rel_select_arr = array();

        //Check for relation fields
        foreach ($fields as $key => $rel_field) {            

            if (isset($rel_field['relation']) && !isset($rel_field['relation']['save_table'])) {

                $rfield = $rel_field['relation'];

                //지점의 경우 Session에 있는 pt_code로 partner_branch 테이블 선택
                if ($pt_code && $rfield['table'] == 'partner_branch' || $pt_code && $rfield['table'] == 'sales_record') {
                    $rtable = $pt_code.'_'.$rfield['table'];
                    $rtable_name = $pt_code.'_'.$rfield['table'].'_'.$key;
                }else{
                    $rtable = $rfield['table'];
                    $rtable_name = $rfield['table'].'_'.$key;
                }

                // $result_query .= " LEFT JOIN  " . $rtable . " as ".$rtable_name." ON " . $table . '.' . $key . "=" . $rtable_name . "." . $rfield['primary_key'] . "  ";

                $result_query_rel .= " LEFT JOIN  " . $rtable . " as ".$rtable_name." ON " . $table . '.' . $key . "=" . $rtable_name . "." . $rfield['primary_key'] . "  ";

                //$this->db->join($rfield['table'], $table.'.'.$key.'='.$rfield['table'].'.'.$rfield['primary_key'], 'left');

                if (isset($rfield['column']) && $rfield['column']) {                

                    $rColumn = $rfield['column'];

                    foreach ($rColumn as $sc_value) {

                        // $rel_select_arr[] = $rtable_name.'.' . $sc_value . ' AS ' . $rtable.'_' . $sc_value;

                        $rel_select_arr[] = $rtable_name.'.' . $sc_value;

                    }
                }else{                    

                    $rtable_schema = $this->schema($rtable);

                    foreach ($rtable_schema as $sc_key => $sc_value) {

                        $rel_select_arr[] = $rtable_name.'.' . $sc_value->Field;

                    }

                }

            }
        }

        $rel_select = '';

        if (count($rel_select_arr)> 0) {
            $rel_select = implode(", ", $rel_select_arr);
        }

        if ($rel_select) {
            $result_query = "SELECT ".$table.".*, ".$rel_select.", ".$table.".".$pk." as pk_id FROM " . $table.$result_query_rel;     
        }else{
            $result_query = "SELECT *, ".$table.".".$pk." as pk_id FROM " . $table;      
        }
          

        if ($where) {

            $result_query .= " WHERE (".$table.".deleted_at = '". $this->deleted_val ."' OR ".$table.".deleted_at IS NULL) AND (";
            $i = 0;
            foreach ($where as $key => $value) {
                if ($i > 0)
                    $result_query .= " AND ";

                //Check if operator is different from = (equal sign)
                $operator_arr = explode(" ", $key);
                if (isset($operator_arr[1]))
                    $operator = $operator_arr[1];
                else
                    $operator = "=";

                if($is_search){
                    $operator = " LIKE ";
                }

                if (is_array($value)) {
                // TODO 데이터셋에서 조건을 걸 때 키 값과 조건이 동일할 경우 배열로 처리
                // 배열인 경우 분기처리
                    $j = 0;

                    foreach ($value as $op_key => $op_value) {
                        if ($j > 0)
                            $result_query .= " AND ";

                        $result_query .= $table.".".$operator_arr[0].$operator."'".$op_value."'";

                        $j++;
                    }

                }else{

                    $result_query .= $table.".".$operator_arr[0].$operator."'".$value."'";

                }

                

                //$this->db->where($key, $value);
                $i++;
            }
            $result_query .= ")";
        }else{
            $result_query .= " WHERE (".$table.".deleted_at = '". $this->deleted_val ."' OR ".$table.".deleted_at IS NULL) ";
        }


        if ($request->getPost('table_search')) {

            $allEmpty = true;
            $tempQuery = ' AND '; //deleted_at 쿼리
            $i = 0;
            // if ($where)
            //     $tempQuery .= " AND ";
            // else
            //     $tempQuery .= " WHERE ";

            //$tempQuery .= " ( ";

            foreach ($schema as $column) {

                if ($request->getPost($column->Field) == '')
                    continue;

                $allEmpty = false;
                $col_search = [];
                $col_search[] = $column->Field;                   
                
                if (isset($fields[$column->Field]['relation']) && isset($fields[$column->Field]['relation']['save_table'])) {        

                    //Search relational table to get the ids of related ids
                    $relField = $fields[$column->Field]['relation'];

                    $parent_table = $relField['table'];
                    
                    $relation_table = $relField['save_table'];
                    $joinString = $relation_table . '.' . $relField['child_field'] . '=' . $parent_table . '.' . $relField['primary_key'];
                    $likeColumns = $relField['display'];
                    $likeTerm = $request->getPost($column->Field);
                    //$relselect is optional. when used it will add DISTINCT to prevent dublicates
                    $relSelect = $relation_table . '.' . $relField['parent_field'];

                    if (isset($fields[$column->Field]['filter']) && $fields[$column->Field]['filter'] && $fields[$column->Field]['type'] == 'multiselect') {
                        $relatedItems = $this->filterRelatedItems($parent_table, $relation_table, $joinString, $likeColumns, $likeTerm, $this->uri->getSegment(2), $column->Field, $relSelect);
                    }else{                        
                        $relatedItems = $this->searchRelatedItems($parent_table, $relation_table, $joinString, $likeColumns, $likeTerm, $relSelect);
                    }
                    
                    $relatedItemsIdArr = [];
                    if (!$relatedItems)
                        $relatedItemsIdArr = '-1';
                    else {
                        //Create an array of ids for whereIn statement
                        foreach ($relatedItems as $relatedItem) {
                            $relatedItemsIdArr[] = $relatedItem->{$relField['parent_field']};
                        }
                    }

                    if ($i > 0)
                        $tempQuery .= " AND ";

                    if (is_array($relatedItemsIdArr))
                        $relTempQuery = '' . $table . '.' . $pk . ' IN (' . implode(',', $relatedItemsIdArr) . ')';
                    else
                        $relTempQuery = $table . '.' . $pk . ' = ' . $relatedItemsIdArr;

                    //  echo $tempQuery.'<br>';
                    $tempQuery .= $relTempQuery;
                    //  echo $tempQuery;

                    $i++;
                    //$allEmpty = false;
                    continue;
                } else if (isset($fields[$column->Field]['relation'])) {                    

                    $col_search = $fields[$column->Field]['relation']['display'];
                    //check if display is an array of columns
                    if (!is_array($col_search))
                        $col_search[] = $col_search;

                    $table_search      = $fields[$column->Field]['relation']['table'];

                    $table_search_name = $table_search.'_'.$column->Field;

                } else if ($column->Type == 'datetime') {

                    $table_search = $table;
                    $table_search_name = $table;

                } else if (isset($fields[$column->Field]['callback']) && ($fields[$column->Field]['callback'] == 'callback_DecryptToCheck' || $fields[$column->Field]['callback'] == 'callback_sms_modal')){

                    $col_search = [];

                }else {
                    $table_search = $table;  
                    $table_search_name = $table;                 

                    //$col_search[] = $column->Field;
                }

                
                

                if ($i > 0)
                    $tempQuery .= " AND ";


                


      

                if (isset($fields[$column->Field]['callback']) && ($fields[$column->Field]['callback'] == 'callback_DecryptToCheck' || $fields[$column->Field]['callback'] == 'callback_sms_modal')) {
                    $table_search = $table;  
                    $table_search_name = $table;  
                    $table_search_field = $this->encryptor->EncryptToCheck($request->getPost($column->Field));
                    
                    $searchLikeTempQueryArr[] = $this->generateEqualClause($table_search, $table_search_name, $column->Field, $table_search_field);      

                }

                if (count($col_search) > 1) {
                    $searchLikeTempQuery = implode(' OR ', $searchLikeTempQueryArr);
                    $searchLikeTempQuery = "($searchLikeTempQuery)";
                } else
                    $searchLikeTempQuery = $searchLikeTempQueryArr[0];



                    if ($column->Type == 'datetime') {


                        $fr_date = $request->getPost('fr_date');
                        $to_date = $request->getPost('to_date');
    
                        if ($fr_date || $to_date) {
    
                            $searchLikeTempQuery = '';
                            $searchLikeTempQueryArr = [];
                            $searchLikeTempQueryArr[] = $this->generateDateClause($table_search, $column->Field, $fr_date, $to_date);
                        }
    
                        
    
                    
                    }else{
    
                        //For loop is required when search must be performed in multiple relational columns from another table
                        $searchLikeTempQuery = '';
                        $searchLikeTempQueryArr = [];
                        foreach ($col_search as $colToSearch) {
    
                            if (isset($fields[$column->Field]['filter']) && $fields[$column->Field]['filter']){
    
                                $searchLikeTempQueryArr[] = $this->generateEqualClause($table_search, $table_search_name, $colToSearch, $request->getPost($column->Field));
    
                            }else{
                                $searchLikeTempQueryArr[] = $this->generateLikeClause($table_search, $table_search_name, $colToSearch, $request->getPost($column->Field));
                            }
                            
                        }
    
                    }



                $tempQuery .= $searchLikeTempQuery;

                //$this->db->like($table_search.'.'.$col_search, $request->getPost($column->Field), 'both');
                $i++;
            }

            $tempQuery .= isset($searchLikeTempQuery) ?  "  " : "";

            if (!$allEmpty)
                $result_query .= $tempQuery;
        }

        if ($order) {
            $result_query .= " ORDER BY ";
            $i = 0;
            
            foreach ($order as $ord) {
                if ($i > 0)
                    $result_query .= ", ";
                $result_query .= $ord[0] . " " . $ord[1];
                //$this->db->order_by($ord[0], $ord[1]);
                $i++;
            }

            
        } else {

            $result_query .= " ORDER BY " . $pk . " DESC";
            //$this->db->order_by($pk, 'DESC');
        }

        $result_qsdsuery = rtrim($result_query, ',');

        if ($offset || $per_page) {
            
            $result_query .= " LIMIT $offset, $per_page ";
        
        }
        //$this->db->limit($per_page, $offset);

        // echo $result_query;

        $page_items = $this->db->query($result_query)->getResult();
        
        return $page_itedsadsadsadms;
    }

    public function getRelationItems($table, $where = null, $orderField = null, $orderDirection = null)
    {
        $builder = $this->db->table($table);

        $builder->where(" (deleted_at = '{$this->deleted_val}' OR deleted_at IS NULL) ");

        if ($where)
            $builder->where($where);

        if ($orderField)
            $builder->orderBy($orderField, $orderDirection);

        $items = $builder->get()
            ->getResult();

        return $items;
    }


    public function get_items($table, $where = null, $orderField = null, $orderDirection = null, $limit = null, $group = null)
    {
        $builder = $this->db->table($table);

        $builder->where(" (deleted_at = '{$this->deleted_val}' OR deleted_at IS NULL) ");

        if ($where)
            $builder->where($where);

        if ($orderField)
            $builder->orderBy($orderField, $orderDirection);


        if ($group) {
            $builder->groupBy($group);    
        }
    
        if ($limit) {
            $builder->limit($limit);
        }

        $items = $builder->get()->getResult();

        return $items;

    }

    public function get_one_item($table, $where = null, $orderField = null, $orderDirection = null, $limit = null, $group = null)
    {
        $builder = $this->db->table($table);

        $builder->where(" (deleted_at = '{$this->deleted_val}' OR deleted_at IS NULL) ");

        if ($where)
            $builder->where($where);

        if ($orderField)
            $builder->orderBy($orderField, $orderDirection);


        if ($group) {
            $builder->groupBy($group);    
        }
    
        if ($limit) {
            $builder->limit($limit);
        }

        $items = $builder->get()->getRow();

        return $items;

    }

    public function getRelationArrayItems($table, $where = null, $whereInField = null, $whereInValue = null)
    {
        $builder = $this->db->table($table);

        $builder->where(" (deleted_at = '{$this->deleted_val}' OR deleted_at IS NULL) ");

        if ($where)
            $builder->where($where);

        if ($whereInField && $whereInValue)
            $builder->whereIn($whereInField, $whereInValue);

        $items = $builder->get()
            ->getResult();

        return $items;
    }

    public function softDeleteItems($table, $where = null, $whereInField = null, $whereInValue = null)
    {
        $builder = $this->db->table($table);

        if ($where)
            $builder->where($where);

        if ($whereInField && $whereInValue)
            $builder->whereIn($whereInField, $whereInValue);

        return $builder->update(['deleted_at' => date('Y-m-d H:i:s')]);
    }

    public function deleteItems($table, $where = null, $whereInField = null, $whereInValue = null)
    {
        $builder = $this->db->table($table);

        if ($where)
            $builder->where($where);

        if ($whereInField && $whereInValue)
            $builder->whereIn($whereInField, $whereInValue);

        return $builder->delete();
    }

    public function batchInsert($table, $data)
    {
        $builder = $this->db->table($table);
        return $builder->insertBatch($data);
    }

    public function getRelationItemsJoin($table, $where, $join_table, $join_string)
    {

        // print_r($where);
        $builder = $this->db->table($table);

        $builder->where(" (".$table.".deleted_at = '{$this->deleted_val}' OR ".$table.".deleted_at IS NULL) ");

        $builder->where($where);
        $builder->join($join_table, $join_string);
        return $builder->get()->getResult();
    }

    // TODO getColumns() 필드 목록 가져오기 
    // public function getRelationItemsJoin($table, $where, $join_table, $join_string)
    // {
    //     $builder = $this->db->table($table);
    //     $builder->where($where);
    //     $builder->join($join_table, $join_string);
    
    //     // Get the column names from the two tables
    //     $columns = array_merge($builder->getTableColumns($table), $builder->getTableColumns($join_table));
    
    //     // Add the table name to each column name
    //     foreach ($columns as $key => $column) {
    //         $columns[$key] = $table . '.' . $column;
    //     }
    
    //     // Set the select columns
    //     $builder->select($columns);
    
    //     // Get the results
    //     return $builder->get()->getResult();
    // }

    public function getFieldInfo($table)
    {
        $query = "SHOW COLUMNS FROM $table";
        $result = $this->db->query($query)->getResultArray();
        return $result;
    }


    public function getFieldTypes($table)
    {
        // $statement = $this->adapter->createStatement('SHOW FIELDS FROM `' . $tableName . '`');

        // $results = iterator_to_array($statement->execute());
        $query = "SHOW COLUMNS FROM $table";
        // $results = iterator_to_array($this->db->query($query)->getResult());
        $results = $this->db->query($query)->getResultArray();
        $fieldTypes = [];
        foreach ($results as $column) {

            $tmpColumn = new ModelFieldType();
            // print_r($column);
            $tmpColumn->isNullable = $column['Null'] == 'YES';
            list($tmpColumn->dataType) = explode('(', $column['Type']);
            $tmpColumn->defaultValue = $column['Default'];
            $tmpColumn->permittedValues = null;

            if ($tmpColumn->dataType === 'enum') {

                $tmpColumn->permittedValues = explode("','", str_replace(['enum(\'', '\')'], '', $column['Type']));
            } else if ($tmpColumn->dataType === 'varchar' || $tmpColumn->dataType === 'char') {
                $tmpColumn->options = (object)[
                    'maxLength' => str_replace(['varchar(', 'char(', ')'], '', $column['Type'])
                ];
            }

            $fieldTypes[$column['Field']] = $tmpColumn;
        }

        $this->_fieldTypes[$table] = $fieldTypes;

        return $fieldTypes;
    }

    public function searchRelatedItems($parent_table, $relation_table, $joinString, $likeColumns, $likeTerm, $select = '*')
    {
        $builder = $this->db->table($relation_table);
        $builder->select($select);
        $builder->join($parent_table, $joinString);

        $builder->where(" ( ". $parent_table .".deleted_at = '{$this->deleted_val}' OR ". $parent_table .".deleted_at IS NULL) ");

        $builder->where(" ( ". $relation_table .".deleted_at = '{$this->deleted_val}' OR ". $relation_table .".deleted_at IS NULL) ");

        for ($i = 0; $i < count($likeColumns); $i++) {
            if ($i < 1) {
                $builder->like($parent_table . '.' . $likeColumns[$i], $likeTerm);
            } else {
                $builder->orLike($parent_table . '.' . $likeColumns[$i], $likeTerm);
            }
        }
        

        return $builder->distinct()->get()->getResult();
    }

    public function filterRelatedItems($parent_table, $relation_table, $joinString, $likeColumns, $likeTerm, $route_url, $fl_name, $select = '*')
    {
        $builder = $this->db->table($relation_table);
        $builder->select($select);
        $builder->join($parent_table, $joinString);

        $builder->where(" (".$parent_table.".deleted_at = '{$this->deleted_val}' OR ".$parent_table.".deleted_at IS NULL) ");

        $builder->where(" (".$relation_table.".deleted_at = '{$this->deleted_val}' OR ".$relation_table.".deleted_at IS NULL) ");

        $builder->where([$relation_table . '.route_url' => $route_url, $relation_table . '.fl_name' => $fl_name]);

        for ($i = 0; $i < count($likeColumns); $i++) {
            $builder->where([$parent_table . '.' . $likeColumns[$i] => $likeTerm]);
        }

        return $builder->distinct()->get()->getResult();
    }


    public function generateLikeClause($table_search, $table_search_name, $colToSearch, $searchTerm)
    {
        return $table_search_name . "." . $colToSearch
            . " LIKE '%"
            . trim($this->db->escapeLikeString($searchTerm))
            . "%' ESCAPE '!'";
    }

    public function generateEqualClause($table_search, $table_search_name, $colToSearch, $searchTerm)
    {
        $pt_code = session()->get('pt_code');

        $tb = $table_search;
        
        if ($pt_code && ($table_search == 'partner_branch' || $table_search == 'sales_record')) {
            $tb = $pt_code.'_'.$table_search;
            $table_search_name = $pt_code.'_'.$table_search_name;
        }

        return $table_search_name . "." . $colToSearch
            . " = '"
            . trim($this->db->escapeLikeString($searchTerm))
            . "'";
    }

    public function generateDateClause($table_search, $colToSearch, $fr_date, $to_date)
    {
        if ($fr_date && $to_date) {
            $search_date = "(".$table_search . "." . $colToSearch . " >= '" . $fr_date . " 00:00:00' and " . $table_search . "." . $colToSearch . " <= '". $to_date . " 23:59:59')";
        }elseif($fr_date && !$to_date){
            $search_date = "(".$table_search . "." . $colToSearch . " >= '" . $fr_date . " 00:00:00' and " . $table_search . "." . $colToSearch . " <= '". $fr_date . " 23:59:59')";
        }
        elseif(!$fr_date && $to_date){
            $search_date = "(".$table_search . "." . $colToSearch . " >= '" . $to_date . " 00:00:00' and " . $table_search . "." . $colToSearch . " <= '". $to_date . " 23:59:59')";
        }
        return $search_date;
    }

    public function getCustomItems($table, $where = null, $request, $schema, $fields, $order, $offset, $per_page)
    {
        //지점의 경우 Session에 있는 pt_code로 partner_branch 테이블 선택
        $pt_code = session()->get('pt_code');

        // TODO relation table 사용시 pk가 중복되어 pk_id로 구분.
        $pk = $this->get_primary_key_field_name($table);
        $result_query = "SELECT *, ".$table.".".$pk." as pk_id FROM " . $table;
        //get primary_key field name
 
        if ($where) {
            $result_query .= " WHERE (".$table.".deleted_at = '". $this->deleted_val ."' OR ".$table.".deleted_at IS NULL) AND (";
            $i = 0;
            foreach ($where as $key => $value) {
                if ($i > 0)
                    $result_query .= " AND ";

                //Check if operator is different from = (equal sign)
                $operator_arr = explode(" ", $key);
                if (isset($operator_arr[1]))
                    $operator = $operator_arr[1];
                else
                    $operator = "=";

                $result_query .= "  `$operator_arr[0]`$operator'$value' ";

                //$this->db->where($key, $value);
                $i++;
            }
            $result_query .= ")";
        }else{
            $result_query .= " WHERE (".$table.".deleted_at = '". $this->deleted_val ."' OR ".$table.".deleted_at IS NULL) ";
        }


        // if ($request->getPost('table_search')) {

        //     $allEmpty = true;
        //     $tempQuery = '';
        //     $i = 0;
        //     if ($where)
        //         $tempQuery .= " AND ";
        //     else
        //         $tempQuery .= " WHERE ";

        //     //$tempQuery .= " ( ";

        //     foreach ($schema as $column) {

        //         if ($request->getPost($column->Field) == '')
        //             continue;

        //         $allEmpty = false;
        //         $col_search = [];
        //         $col_search[] = $column->Field;
        //         if (isset($fields[$column->Field]['relation']) && isset($fields[$column->Field]['relation']['save_table'])) {

                    

        //             //Search relational table to get the ids of related ids
        //             $relField = $fields[$column->Field]['relation'];

        //             $parent_table = $relField['table'];
        //             $relation_table = $relField['save_table'];
        //             $joinString = $relation_table . '.' . $relField['child_field'] . '=' . $parent_table . '.' . $relField['primary_key'];
        //             $likeColumns = $relField['display'];
        //             $likeTerm = $request->getPost($column->Field);
        //             //$relselect is optional. when used it will add DISTINCT to prevent dublicates
        //             $relSelect = $relation_table . '.' . $relField['parent_field'];

        //             if (isset($fields[$column->Field]['filter']) && $fields[$column->Field]['filter'] && $fields[$column->Field]['type'] == 'multiselect') {
        //                 $relatedItems = $this->filterRelatedItems($parent_table, $relation_table, $joinString, $likeColumns, $likeTerm, $this->uri->getSegment(2), $column->Field, $relSelect);
        //             }else{                        
        //                 $relatedItems = $this->searchRelatedItems($parent_table, $relation_table, $joinString, $likeColumns, $likeTerm, $relSelect);
        //             }
                    
        //             $relatedItemsIdArr = [];
        //             if (!$relatedItems)
        //                 $relatedItemsIdArr = '-1';
        //             else {
        //                 //Create an array of ids for whereIn statement
        //                 foreach ($relatedItems as $relatedItem) {
        //                     $relatedItemsIdArr[] = $relatedItem->{$relField['parent_field']};
        //                 }
        //             }

        //             if ($i > 0)
        //                 $tempQuery .= " AND ";

        //             if (is_array($relatedItemsIdArr))
        //                 $relTempQuery = '' . $table . '.' . $pk . ' IN (' . implode(',', $relatedItemsIdArr) . ')';
        //             else
        //                 $relTempQuery = $table . '.' . $pk . ' = ' . $relatedItemsIdArr;

        //             //  echo $tempQuery.'<br>';
        //             $tempQuery .= $relTempQuery;
        //             //  echo $tempQuery;

        //             $i++;
        //             //$allEmpty = false;
        //             continue;
        //         } else if (isset($fields[$column->Field]['relation'])) {

                    

        //             $col_search = $fields[$column->Field]['relation']['display'];
        //             //check if display is an array of columns
        //             if (!is_array($col_search))
        //                 $col_search[] = $col_search;

        //             $table_search = $fields[$column->Field]['relation']['table'];

        //         } else if ($column->Type == 'datetime') {

        //             $table_search = $table;

        //         }else {
        //             $table_search = $table;

        //             //$col_search[] = $column->Field;
        //         }
        //         if ($i > 0)
        //             $tempQuery .= " AND ";


                


        //         if ($column->Type == 'datetime') {


        //             $fr_date = $request->getPost('fr_date');
        //             $to_date = $request->getPost('to_date');

        //             if ($fr_date || $to_date) {

        //                 $searchLikeTempQuery = '';
        //                 $searchLikeTempQueryArr = [];
        //                 $searchLikeTempQueryArr[] = $this->generateDateClause($table_search, $column->Field, $fr_date, $to_date);
        //             }

                    

                
        //         }else{

        //             //For loop is required when search must be performed in multiple relational columns from another table
        //             $searchLikeTempQuery = '';
        //             $searchLikeTempQueryArr = [];
        //             foreach ($col_search as $colToSearch) {

        //                 if (isset($fields[$column->Field]['filter']) && $fields[$column->Field]['filter']){

        //                     $searchLikeTempQueryArr[] = $this->generateEqualClause($table_search, $colToSearch, $request->getPost($column->Field));

        //                 }else{
        //                     $searchLikeTempQueryArr[] = $this->generateLikeClause($table_search, $colToSearch, $request->getPost($column->Field));
        //                 }
                        
        //             }

        //         }




        //         if (count($col_search) > 1) {
        //             $searchLikeTempQuery = implode(' OR ', $searchLikeTempQueryArr);
        //             $searchLikeTempQuery = "($searchLikeTempQuery)";
        //         } else
        //             $searchLikeTempQuery = $searchLikeTempQueryArr[0];


        //         $tempQuery .= $searchLikeTempQuery;

        //         //$this->db->like($table_search.'.'.$col_search, $request->getPost($column->Field), 'both');
        //         $i++;
        //     }

        //     $tempQuery .= isset($searchLikeTempQuery) ?  "  " : "";

        //     if (!$allEmpty)
        //         $result_query .= $tempQuery;
        // }

        if ($order) {
            $result_query .= " ORDER BY ";
            $i = 0;
            
            foreach ($order as $ord) {
                if ($i > 0)
                    $result_query .= ", ";
                $result_query .= $ord[0] . " " . $ord[1];
                //$this->db->order_by($ord[0], $ord[1]);
                $i++;
            }

            
        } else {

            $result_query .= " ORDER BY " . $pk . " DESC";
            //$this->db->order_by($pk, 'DESC');
        }

        $result_query = rtrim($result_query, ',');
        $result_query .= " LIMIT $offset, $per_page ";
        //$this->db->limit($per_page, $offset);
        $page_items = $this->db->query($result_query)->getResult();

        return $page_items;
    }

    public function next_wr_num($table)
    {
        $builder = $this->db->table($table);

        $builder->selectMin('wr_num');
		$row = $builder->get()->getRow();
		$row->wr_num = (isset($row->wr_num)) ? $row->wr_num : 0;
		$wr_num = $row->wr_num - 1;

		return $wr_num;

    }

    public function get_reply_char($table, $origin, $reply_len)
    {
        $builder = $this->db->table($table);

        $builder->select('MAX(SUBSTRING(wr_reply, ' . $reply_len . ', 1)) as reply', false);

		$builder->where('wr_num', $origin->wr_num);
		$builder->where('SUBSTRING(wr_reply, ' . $reply_len . ', 1) <>', '');
		if ($origin->id) {   
            if ($origin->wr_reply) {
                $builder->like('wr_reply', $origin->wr_reply, 'after');
            }        			
		}

        return $builder->get()->getRow();

    }


}