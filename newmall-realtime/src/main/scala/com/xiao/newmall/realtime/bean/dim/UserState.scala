package com.xiao.newmall.realtime.bean.dim

case class UserState(userId:String, //保存phoenix字段只能String类型
                     ifConsumed:String
                    )
