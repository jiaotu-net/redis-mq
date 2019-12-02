<?php
namespace JiaoTu\RedisMQ;

/**
 * 
 * 最原始的队列消费者
 * 无任何消息安全性保障, 简单粗暴.
 * 它整个过程都是无锁的, 所以性能最高.
 * @author haohui.wang
 *
 */
class Consumer {
    /**
     * Redis客户端
     * @var \JiaoTu\RedisProvider\Redis
     */
    protected $redis = null;
    
    /**
     * 封包协议
     * @JiaoTubrary\RedisMQ\Protocol
     */
    protected $protocol = null;
    
    /**
     * 队列KEY名
     * @var string
     */
    protected $channel = '';
    
    /**
     * 构造
     * @param \JiaoTu\RedisProvider\Redis $redis redis客户端
     * @param string $channel 队列KEY名
     * @param \JiaoTu\RedisMQ\Protocol $protocol 封包协议
     * @throws \JiaoTu\RedisMQ\RedisMQException
     */
    public function __construct(& $redis, $channel, & $protocol) {
        if ('' == $channel) {
            throw new RedisMQException('$channel不能为空', RedisMQException::CODE_PARAM_ERROR);
        }
        
        if (!$protocol instanceof Protocol) {
            throw new RedisMQException('$protocol类型错误', RedisMQException::CODE_TYPE_ERROR);
        }
        
        if (!$redis instanceof \JiaoTu\RedisProvider\Redis) {
            throw new RedisMQException('$redis类型错误', RedisMQException::CODE_TYPE_ERROR);
        }
        
        $this->redis = & $redis;
        $this->channel = $channel;
        $this->protocol = & $protocol;
    }
    
    /**
     * 消费一条消息
     * 若队列为空, 则返回null; 若队列不为空, 则获取一条消息并将消息从队列中移除.
     * @throws \JiaoTu\RedisMQ\RedisMQException
     * @return null|\JiaoTu\RedisMQ\Message
     */
    public function consume() {
        try {
            $data = $this->redis->rPop($this->channel);
        } catch (\RedisException $e) {
            throw new RedisMQException('Redis操作失败', RedisMQException::CODE_REDIS_ERROR, $e);
        }
        
        if (empty($data)) {
            return null;
        }
        
        $message = & $this->protocol->decode($data);
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
        try {
            $data = $this->redis->brPop($this->channel, $timeout);
        } catch (\RedisException $e) {
            throw new RedisMQException('Redis操作失败', RedisMQException::CODE_REDIS_ERROR, $e);
        }
        
        if (empty($data) || !is_array($data) || !isset($data[1])) {
            return null;
        }
        
        $message = & $this->protocol->decode($data[1]);
        return $message;
    }
    
    /**
     * 取队列长度
     * @throws \JiaoTu\RedisMQ\RedisMQException
     * @return int 返回队列长度; 队列不存在或数据类型不正确亦返回0.
     */
    public function getLength() {
        try {
            $len = $this->redis->lLen($this->channel);
        } catch (\RedisException $e) {
            throw new RedisMQException('Redis操作失败', RedisMQException::CODE_REDIS_ERROR, $e);
        }
        
        return (int) $len;
    }
}