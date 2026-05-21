package com.hmdp.utils;

import com.hmdp.dto.UserDTO;

public class UserHolder {
    // 单次http请求中的“用户上下文容器“，解决拦截层与业务层之间用户信息传递的问题，并保证线程安全
    private static final ThreadLocal<UserDTO> tl = new ThreadLocal<>();

    public static void saveUser(UserDTO user){
        tl.set(user);
    }

    public static UserDTO getUser(){
        return tl.get();
    }

    public static void removeUser(){
        tl.remove();
    }
}
