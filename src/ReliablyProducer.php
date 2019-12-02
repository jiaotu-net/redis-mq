<?php
namespace JiaoTu\RedisMQ;

/**
 * 可靠的队列生产者 配合 ReliablyProducer 使用
 * 提供"确认"及"恢复"机制, 为消息可靠性提供了保障.
 * "恢复"机制并不保证消息顺序性
 * 由于消息有备份及确认机制, 所以性能略低.
 * 队列无锁, 在一个消息为确认前多个消费者皆可消费消息.
 * 注意: 在低于64位环境下, 此类有异常
 * @author haohui.wang
 *
 */
class ReliablyProducer extends Producer {
    /**
     * message UUID (字节)长度
     * @var integer
     */
    const UUID_LENGTH = 32;
    
    /**
     * UUID生成函数
     * @var callable
     */
    protected $uuidFunction = '';
    
    /**
     * 消息队列备份KEY名
     * @var string
     */
    protected $channelBak = '';
    
    /**
     * 构造
     * @param \JiaoTu\RedisProvider\Redis $redis redis客户端
     * @param string $channel 队列KEY名
     * @param \JiaoTu\RedisMQ\Protocol $protocol 封包协议 
     * @throws \JiaoTu\RedisMQ\RedisMQException
     */
    public function __construct(& $redis, $channel, & $protocol) {
        parent::__construct($redis, $channel, $protocol);
        
        if (PHP_INT_SIZE < 8) {
            throw new RedisMQException('请在64位及以上环境中使用本类', RedisMQException::CODE_ENVT_ERROR);
        }
        
        if (function_exists ( 'random_bytes' )) {
            $this->uuidFunction = 'random_bytes';
        } elseif (function_exists ( 'openssl_random_pseudo_bytes' )) {
            $this->uuidFunction = 'openssl_random_pseudo_bytes';
        } else {
            throw new RedisMQException('缺少openssl扩展支持', RedisMQException::CODE_ENVT_ERROR);
        }
        
        $this->channelBak = $channel . ':BAK';
    }
    
    /**
     * 发布一个消息到队列中
     * @param \JiaoTu\RedisMQ\Message $message 欲发布消息对象
     * @param int $time [optional]时间戳, 指定消息进入队列时的时间戳; 若不传默认为当前时间.
     * @throws \JiaoTu\RedisMQ\RedisMQException
     * @return boolean 成功返回true; 失败返回false.
     */
    public function publish($message) {
        $bytes = ($this->uuidFunction)(self::UUID_LENGTH);
        $message->uuid = bin2hex($bytes);
        $score = time();
        $score <<= 8; // 留出1个字节(0~255)保存消息重试计数
        
        $msgBin = $this->protocol->encode($message);
        
        try {
            $ret = $this->redis->zAdd($this->channelBak, $score, $msgBin);
            if ($ret < 1) {
                return false;
            }
            
            $ret = $this->redis->lPush($this->channel, $msgBin);
            if ($ret < 1) {
                $this->redis->zRem($msgBin);
                return false;
            }
        } catch (\RedisException $e) {
            throw new RedisMQException('Redis操作失败', RedisMQException::CODE_REDIS_ERROR, $e);
        }
        
        return true;
    }
    
    /**
     * 恢复队列中未确认(无论是否消费过)的消息, 重新入队
     * @param int $time 时间戳, 指定欲恢复什么时间之前(含)的消息; 若传0则全部恢复.
     * @param boolean $insert [optional] 是否插入到队首; true 是(默认值); false 否(插入到队尾).
     * @throws \JiaoTu\RedisMQ\RedisMQException
     * @return boolean 成功返回true; 部分成功或失败返回false.
     */
    public function recover($time, $insert = true) {
        if ($time < 0) {
            throw new RedisMQException('$time必须大于等于0', RedisMQException::CODE_PARAM_ERROR);
        }
        
        if(0 == $time) {
            $time = '+inf';
        } else {
            $time <<= 8;
        }
        $offset = 0;
        $limit = 100;
        
        while (true) {
            $msgList = $this->redis->zRangeByScore($this->channelBak, 0, $time, array('limit' => array($offset, $limit)));
            
            if (empty($msgList)) {
                return true;
            }
            
            try {
                foreach ($msgList as $msgBin) {
                    if ($insert) {
                        $ret = $this->redis->rPush($this->channel, $msgBin);
                    } else {
                        $ret = $this->redis->lPush($this->channel, $msgBin);
                    }
                    
                    if ($ret < 1) {
                        return false;
                    }
                }
            } catch (\RedisException $e) {
                throw new RedisMQException('Redis操作失败', RedisMQException::CODE_REDIS_ERROR, $e);
            }
            
            $offset += $limit;
        }
        
        return true;
    }
    
    /**
     * 取队列长度
     * @throws \JiaoTu\RedisMQ\RedisMQException
     * @return int 返回队列长度; 队列不存在或数据类型不正确亦返回0.
     */
    public function getLength() {
        try {
            $len = $this->redis->zCard($this->channelBak);
        } catch (\RedisException $e) {
            throw new RedisMQException('Redis操作失败', RedisMQException::CODE_REDIS_ERROR, $e);
        }
        
        return (int) $len;
    }
}