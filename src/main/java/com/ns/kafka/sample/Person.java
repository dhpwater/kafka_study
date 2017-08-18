package com.ns.kafka.sample;

import com.alibaba.fastjson.JSON;

public class Person {

	private int id ;
	
	private String name ;
	
	private int age ;

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}
	
	@Override
    public String toString() {
        return "Person [id="+ id + ", name="+ name+ ", age="+ age+ "]";
    }
	
	public static void main(String[] args){
		
		Person person = new Person() ;
		person.setId(1);
		person.setAge(11);
		person.setName("ns");
		System.out.println(person);
		
		String s = JSON.toJSONString(person);
		
		System.out.println(s);
		
		Person p = JSON.parseObject(s, Person.class);
		
		System.out.println("xxx");
		System.out.println(p);
		
		
	}
}
