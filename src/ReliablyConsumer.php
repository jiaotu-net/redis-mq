<?php
namespace JiaoTu\RedisMQ;

/**
 * 
 * 可靠的队列消费者
 * 配合 ReliablyProducer 使用
 * 消息重试计数范围0~255, 255后不再增加, 固定为255.
 * 注意: 在低于64位环境下, 此类有异常
 * @author haohui.wang
 *
 */
class ReliablyConsumer extends Consumer {
    /**
     * 消息队列备份KEY名
     * @var string
     */
    protected $channelBak = '';
    
    /**
     * 有消息待确认
     * @var boolean
     */
    protected $ackExpected = false;
    
    /**
     * 未解码消息
     * @var string
     */
    protected $msgBin = '';
    
    /**
     * 未确认消息score值
     * @var integer
     */
    protected $msgScore = 0;
    
    /**
     * 未确认消息重试计数 0~255
     * @var integer
     */
    protected $msgRetryCount = 0;
    
    /**
     * 构造
     * @param \JiaoTu\RedisProvider\Redis $redis redis客户端
     * @param string $channel 队列KEY名
     * @param \JiaoTu\RedisMQ\Protocol $protocol 封包协议
     * @throws \JiaoTu\RedisMQ\RedisMQException
     */
    public function __construct(& $redis, $channel, & $protocol) {
        if (PHP_INT_SIZE < 8) {
            throw new RedisMQException('请在64位及以上环境中使用本类', RedisMQException::CODE_ENVT_ERROR);
        }
        
        parent::__construct($redis, $channel, $protocol);
        
        $this->channelBak = $channel . ':BAK';
    }
    
    /**
     * 消费一条消息
     * 若队列为空, 则返回null; 若队列不为空, 则获取一条消息并将消息从队列中移除.
     * @throws \JiaoTu\RedisMQ\RedisMQException
     * @return null|\JiaoTu\RedisMQ\Message
     */
    public function consume() {
        if ($this->ackExpected) {
            throw new RedisMQException('上一条消息未确认, 使用ack或nack后才能获取新消息', RedisMQException::CODE_MUST_ACK);
        }
        
        try {
            while (true) {
                $this->msgBin = $this->redis->rPop($this->channel);
                if (empty($this->msgBin)) {
                    return null;
                }
                
                $this->msgScore = $this->redis->zScore($this->channelBak, $this->msgBin);
                if(false === $this->msgScore) {
                    /* 这条消息已经ACK过 */
                    continue;
                }
                break;
            }
        } catch (\RedisException $e) {
            throw new RedisMQException('Redis操作失败', RedisMQException::CODE_REDIS_ERROR, $e);
        }
        
        
        $message = & $this->protocol->decode($this->msgBin);
        
        $this->ackExpected = true;
        $this->msgRetryCount = $this->msgScore & 255;
        return $message;
    }
    
    /**
     * 等待消息
     * 若队列为空, 此方法将阻塞,直到超时或获得消息并返回.
     * 获得的消息将被从队列中移除.
     * @param integer $timeout 等待超时的秒数
     * @throws \JiaoTu\RedisMQ\RedisMQException
     * @return null|\JiaoTu\RedisMQ\Message 超时返回null; 成功得到消息则将其返回.
     */
    public function waitMessage($timeout = 0) {
        if ($this->ackExpected) {
            throw new RedisMQException('上一条消息未确认, 使用ack或nack后才能获取新消息', RedisMQException::CODE_MUST_ACK);
        }
        
        try {
            while (true) {
                $data = $this->redis->brPop($this->channel, $timeout);
                if (empty($data) || !is_array($data) || !isset($data[1])) {
                    return null;
                }
                
                $this->msgBin = $data[1];
                
                $this->msgScore = $this->redis->zScore($this->channelBak, $this->msgBin);
                if(false === $this->msgScore) {
                    /* 这条消息已经ACK过 */
                    continue;
                }
                break;
            }
        } catch (\RedisException $e) {
            throw new RedisMQException('Redis操作失败', RedisMQException::CODE_REDIS_ERROR, $e);
        }
        
        $message = & $this->protocol->decode($this->msgBin);
        
        $this->ackExpected = true;
        $this->msgRetryCount = $this->msgScore & 255;
        
        return $message;
    }
    
    /**
     * 确认消息, 移除备份
     * @throws \JiaoTu\RedisMQ\RedisMQException
     * @return boolean 成功返回true; 失败返回false.
     */
    public function ack() {
        if (!$this->ackExpected) {
            throw new RedisMQException('没有消息需要确认', RedisMQException::CODE_NEED_NOT_ACK);
        }
        
        try {
            $ret = $this->redis->zRem($this->channelBak, $this->msgBin);
        } catch (\RedisException $e) {
            throw new RedisMQException('Redis操作失败', RedisMQException::CODE_REDIS_ERROR, $e);
        }
        
        $this->ackExpected = false;
        $this->msgBin = '';
        $this->msgRetryCount = 0;
        return true;
    }
    
    /**
     * 将消息退回队列
     * @param boolean $insert [optional] 是否插入到队首; true 是(默认值); false 否(插入到队尾).
     * @throws \JiaoTu\RedisMQ\RedisMQException
     * @return boolean 成功返回true; 失败返回false.
     */
    public function nack($insert = true) {
        if (!$this->ackExpected) {
            throw new RedisMQException('没有消息需要确认', RedisMQException::CODE_NEED_NOT_ACK);
        }
        
        $score = $this->msgRetryCount < 255 ? $this->msgScore + 1 : $this->msgScore;
        try {
            $this->redis->zAdd($this->channelBak, $score, $this->msgBin);
            
            if ($insert) {
                $ret = $this->redis->rPush($this->channel, $this->msgBin);
            } else {
                $ret = $this->redis->lPush($this->channel, $this->msgBin);
            }
            
            if ($ret < 1) {
                return false;
            }
        } catch (\RedisException $e) {
            throw new RedisMQException('Redis操作失败', RedisMQException::CODE_REDIS_ERROR, $e);
        }
        
        $this->ackExpected = false;
        $this->msgBin = '';
        $this->msgRetryCount = 0;
        
        return true;
    }
    
    /**
     * 保留消息
     * 将当前消息移出消息循环, 不再被消费者处理, 待使用ReliablyProducer 的 recover 方法回到队列循环中.
     * @param boolean|int $plusCount [optional]是否增加重试次数; true是; false否; 若为数字则覆盖消息进入的时间戳且在当前重试计数上增加$plusCount(可传负数做减, 最终重试计数会在0~255); 默认false.
     * @throws \JiaoTu\RedisMQ\RedisMQException
     * @return boolean 成功返回true; 失败返回false.
     */
    public function reserve($plusCount = false) {
        if (!$this->ackExpected) {
            throw new RedisMQException('当前没有待处理消息', RedisMQException::CODE_NEED_NOT_ACK);
        }
        
        $score = false;
        
        if (is_numeric($plusCount)) {
            $score = time() << 8; // 留出1个字节(0~255)保存消息重试计数
            $this->msgRetryCount = $this->msgRetryCount + $plusCount;
            if ($this->msgRetryCount < 0) {
                $this->msgRetryCount = 0;
            } else if ($this->msgRetryCount > 255) {
                $this->msgRetryCount = 255;
            }
            
            $score += $this->msgRetryCount;
        } else if ($plusCount && $this->msgRetryCount < 255) {
            $score = $this->msgScore + 1;
        }
        
        if (false !== $score) {
            try {
                $this->redis->zAdd($this->channelBak, $score, $this->msgBin);
            } catch (\RedisException $e) {
                throw new RedisMQException('Redis操作失败', RedisMQException::CODE_REDIS_ERROR, $e);
            }
        }
        
        $this->ackExpected = false;
        $this->msgBin = '';
        $this->msgRetryCount = 0;
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
    
    /**
     * 获取当前未确认消息重试计数
     */
    public function getMsgRetryCount() {
        return $this->msgRetryCount;
    }
}