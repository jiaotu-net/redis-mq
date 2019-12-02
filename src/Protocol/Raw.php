<?php
namespace JiaoTu\RedisMQ\Protocol;

use JiaoTu\RedisMQ\RedisMQException;
use JiaoTu\RedisMQ\Message;

/**
 * 无编码
 * @author haohui.wang
 *
 */
class Raw extends \JiaoTu\RedisMQ\Protocol {
    /**
     * {@inheritDoc}
     * @see \JiaoTu\RedisMQ\Protocol::decode()
     */
    public function & decode($data) {
        if (!is_object($data)) {
            throw new RedisMQException('欲解码数据类型错误', RedisMQException::CODE_TYPE_ERROR);
        }
        
        $message = new Message();
        foreach ($data as $key => $value) {
            $message->$key = $value;
        }
        
        return $message;
    }

    /**
     * {@inheritDoc}
     * @see \JiaoTu\RedisMQ\Protocol::encode()
     */
    public function encode(\JiaoTu\RedisMQ\Message $message) {
        return $message;
    }
}