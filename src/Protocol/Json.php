<?php
namespace JiaoTu\RedisMQ\Protocol;

use JiaoTu\RedisMQ\RedisMQException;
use JiaoTu\RedisMQ\Message;
use JiaoTu\RedisMQ\Protocol;

/**
 * JSON编码
 * @author haohui.wang
 *
 */
class Json extends Protocol {
    /**
     * 编码时选项值
     * @var integer
     */
    protected $encodeOptions = 0;
    
    /**
     * 解码时选项值
     * @var integer
     */
    protected $decodeOptions = 0;
    
    /**
     * 编解码递归深度
     * @var integer
     */
    protected $depth = 512;
    
    /**
     * 构造
     * @param array $options [optional] 封包协议参数; 选项值有:
     * encode_options JSON编码参数, 见json_encode方法options参数
     * decode_options JSON解码参数, 见json_decode方法options参数
     * depth 递归深度
     * @throws \JiaoTu\RedisMQ\RedisMQException
     */
    public function __construct($options = array()) {
        parent::__construct($options);
        if (isset($options['encode_options'])) {
            $this->encodeOptions = $options['encode_options'];
        }
        
        if (isset($options['decode_options'])) {
            $this->decodeOptions = $options['decode_options'];
        }
        
        if (isset($options['depth'])) {
            $this->depth = $options['depth'];
        }
    }

    /**
     * {@inheritDoc}
     * @see \JiaoTu\RedisMQ\Protocol::decode()
     */
    public function & decode($data) {
        $stdObj = json_decode($data, false, $this->depth, $this->decodeOptions);
        
        if (null === $stdObj) {
            throw new RedisMQException('欲解码数据格式错误', RedisMQException::CODE_FORMAT_ERROR);
        }
        
        $message = new Message();
        foreach ($stdObj as $key => $value) {
            $message->$key = $value;
        }
        
        return $message;
    }

    /**
     * {@inheritDoc}
     * @see \JiaoTu\RedisMQ\Protocol::encode()
     */
    public function encode(\JiaoTu\RedisMQ\Message $message) {
        return json_encode($message, $this->encodeOptions, $this->depth);
    }
}