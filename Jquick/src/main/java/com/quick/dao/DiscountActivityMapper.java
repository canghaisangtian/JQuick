package com.quick.dao;

import com.quick.entity.DiscountActivity;

public interface DiscountActivityMapper {
    int deleteByPrimaryKey(Integer id);

    int insert(DiscountActivity record);

    int insertSelective(DiscountActivity record);

    DiscountActivity selectByPrimaryKey(Integer id);

    int updateByPrimaryKeySelective(DiscountActivity record);

    int updateByPrimaryKey(DiscountActivity record);
}