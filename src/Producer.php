<?php
namespace JiaoTu\RedisMQ;

/**
 * 最原始的队列生产者
 * 无任何消息安全性保障, 简单粗暴.
 * 它整个过程都是无锁的, 所以性能最高.
 * @author haohui.wang
 *
 */
class Producer {
    /**
     * Redis客户端
     * @var \JiaoTu\RedisProvider\Redis
     */
    protected $redis = null;
    
    /**
     * 封包协议
     * @var \JiaoTu\RedisMQ\Protocol
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
     * 发布一个消息到队列中
     * @param \JiaoTu\RedisMQ\Message $message 欲发布消息对象
     * @throws \JiaoTu\RedisMQ\RedisMQException
     * @return boolean 成功返回true; 失败返回false.
     */
    public function publish($message) {
        try {
            $ret = $this->redis->lPush($this->channel, $this->protocol->encode($message));
        } catch (\RedisException $e) {
            throw new RedisMQException('Redis操作失败', RedisMQException::CODE_REDIS_ERROR, $e);
        }
        
        return $ret > 0;
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