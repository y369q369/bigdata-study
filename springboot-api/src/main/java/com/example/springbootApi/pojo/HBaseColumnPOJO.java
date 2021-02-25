package com.example.springbootApi.pojo;

import lombok.Data;

/**
 * @Author grassPrince
 * @Date 2021/2/25 17:27
 * @Description 列组元素数据对象
 **/
@Data
public class HBaseColumnPOJO {

    /** 列族名称 */
    private byte[] family;

    /** 列族元素的名称 */
    private byte[] qualifier;

    /** 列族元素的值 */
    private byte[] value;

}
