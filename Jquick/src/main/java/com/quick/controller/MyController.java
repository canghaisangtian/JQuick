package com.quick.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/controller")
public class MyController {
	
	
	
	
	
	
	@RequestMapping("/begin")
	@ResponseBody
	public String begin() {
		return "hello world";
	}

}
