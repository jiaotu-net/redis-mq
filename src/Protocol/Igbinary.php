<?php
namespace JiaoTu\RedisMQ\Protocol;

use JiaoTu\RedisMQ\RedisMQException;
use JiaoTu\RedisMQ\Message;
use JiaoTu\RedisMQ\Protocol;

/**
 * igbinary编码
 * @author haohui.wang
 *
 */
class Igbinary extends Protocol {
    /**
     * {@inheritDoc}
     * @see \JiaoTu\RedisMQ\Protocol::decode()
     */
    public function __construct($options = array()) {
        parent::__construct($options);
        if (!extension_loaded('igbinary')) {
            throw new RedisMQException('缺少igbinary扩展支持', RedisMQException::CODE_ENVT_ERROR);
        }
    }

    /**
     * {@inheritDoc}
     * @see \JiaoTu\RedisMQ\Protocol::decode()
     */
    public function & decode($data) {
        $message = igbinary_unserialize($data);
        if (!$message instanceof Message) {
            throw new RedisMQException('欲解码数据格式错误', RedisMQException::CODE_FORMAT_ERROR);
        }
        return $message;
    }

    /**
     * {@inheritDoc}
     * @see \JiaoTu\RedisMQ\Protocol::encode()
     */
    public function encode(\JiaoTu\RedisMQ\Message $message) {
        return igbinary_serialize($message);
    }
}