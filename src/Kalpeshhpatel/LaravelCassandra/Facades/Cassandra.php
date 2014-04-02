<?php
/**
 * Created by PhpStorm.
 * User: realmile
 * Date: 9/1/14
 * Time: 6:16 PM
 */

namespace Kalpeshhpatel\LaravelCassandra\Facades;

use Illuminate\Support\Facades\Facade;

class Cassandra extends Facade  {

    protected static function getFacadeAccessor() {
        return 'Cassandra';
    }
}