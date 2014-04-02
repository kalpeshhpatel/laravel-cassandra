Laravel Cassandra
=========

Laravel package for working with cassandra.

Installation
----

Update your `composer.json` file to include this package as a dependency
```json
"kalpeshhpatel/laravel-cassandra": "dev-master"
```

Register the Cassandra service provider by adding it to the providers array in the `app/config/app.php` file.
```
Kalpeshhpatel\LaravelCassandra\LaravelCassandraServiceProvider
```

# Configuration

Copy the config file into your project by running
```
php artisan config:publish kalpeshhpatel/laravel-cassandra
```

This will generate a config file like this
```
return array(
    
    /**
     * Update this with your node IP on which cassandra is running
     */
    'cassandra_node_ip' => '127.0.0.1',
    
    /**
     * Your application keyspace
     */
    'keyspace' => 'MyKeySpace'
);
```

# Usage

**Coming soon.

This package is basically wrapper of [PHPCassa] libray in Laravel terms.

####Usage advice
This package should be used with [Laravel Queues], so pushes dont blocks the user and are processed in the background, meaning a better flow.

[PHPCassa]:https://github.com/thobbs/phpcassa
