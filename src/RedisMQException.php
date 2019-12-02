<?php
namespace JiaoTu\RedisMQ;

/**
 * RedisMQ异常
 * @author haohui.wang
 *
 */
class RedisMQException extends \Exception {
    /**
     * 环境错误
     * @var integer
     */
    const CODE_ENVT_ERROR = 1;
    
    /**
     * 类型错误
     * @var integer
     */
    const CODE_TYPE_ERROR = 2;
    
    /**
     * 格式错误
     * @var integer
     */
    const CODE_FORMAT_ERROR = 3;
    
    /**
     * 参数错误
     * @var integer
     */
    const CODE_PARAM_ERROR = 4;
    
    /**
     * REDIS错误
     * @var integer
     */
    const CODE_REDIS_ERROR = 5;
    
    /**
     * 消息必须确认
     * @var integer
     */
    const CODE_MUST_ACK = 1001;
    
    /**
     * 消息无须确认
     * @var integer
     */
    const CODE_NEED_NOT_ACK = 1002;
}