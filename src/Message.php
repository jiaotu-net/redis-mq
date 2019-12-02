<?php

namespace JiaoTu\RedisMQ;

/**
 * 队列消息体
 *
 * @author haohui.wang
 *        
 */
class Message {
    /**
     * 唯一ID
     *
     * @var string
     */
    public $uuid = '';
    
    /**
     * 消息正文
     *
     * @var mixed
     */
    public $content = null;
    
    /**
     * 构造
     *
     * @param mixed $content [optional] 消息正文
     */
    public function __construct($content = null) {
        $this->content = $content;
    }
}