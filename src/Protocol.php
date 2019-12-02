<?php
namespace JiaoTu\RedisMQ;

/**
 * 封包协议
 * @author haohui.wang
 *
 */
abstract class Protocol {
    /**
     * 选项
     * @var array
     */
    protected $options = array();
    
    /**
     * 构造
     * @param array $options [optional] 封包协议参数
     * @throws \JiaoTu\RedisMQ\RedisMQException
     */
    public function __construct($options = array()) {
        $this->options = $options;
    }
    
    /**
     * 编码
     * @param \JiaoTu\RedisMQ\Message $message 消息
     * @throws \JiaoTu\RedisMQ\RedisMQException
     * @return string 编码后的数据
     */
    public abstract function encode(\JiaoTu\RedisMQ\Message $message);
    
    /**
     * 解码
     * @param string $data 欲解码的数据
     * @throws \JiaoTu\RedisMQ\RedisMQException
     * @return \JiaoTu\RedisMQ\Message 成功返回消息体
     */
    public abstract function & decode($data);
}