package com.hmdp.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.entity.User;

import javax.servlet.http.HttpSession;

public interface IUserService extends IService<User> {

    Result sendCodeSession(String phone, HttpSession session);

    Result sendCodeRedis(String phone, HttpSession session);

    Result loginSession(LoginFormDTO loginForm, HttpSession session);

    Result loginRedis(LoginFormDTO loginForm, HttpSession session);

    Result sign();

    Result signCount();

}
