#[macro_use]
extern crate cdrs;
#[macro_use]
extern crate cdrs_helpers_derive;
#[allow(unused_variables)]

use cdrs::authenticators::NoneAuthenticator;
use cdrs::cluster::{ClusterTcpConfig, NodeTcpConfigBuilder, TcpConnectionPool};
use cdrs::cluster::session::{new as new_session, Session};
use cdrs::frame::IntoBytes;
use cdrs::load_balancing::RoundRobin;
use cdrs::query::*;
use cdrs::types::from_cdrs::FromCDRSByName;
use cdrs::types::prelude::*;
use std::io;
//use std::io::Read;

type CurrentSession = Session<RoundRobin<TcpConnectionPool<NoneAuthenticator>>>;

fn main() {
    let node = NodeTcpConfigBuilder::new("127.0.0.1:9042", NoneAuthenticator {}).build();
    let cluster_config = ClusterTcpConfig(vec![node]);
    let no_compression: CurrentSession = new_session(&cluster_config, RoundRobin::new()).expect("session should be created");

    create_keyspace(&no_compression);
    create_udt(&no_compression);
    create_table(&no_compression);

    let mut preference = "Y".to_string();

    while preference != "n".to_string() && preference != "N".to_string() {

        println!("operations can be perform are :\n 1.insertion\n 2.update\n3.delete\n4.show\n enter ur choice:");
        let mut choice = String::new();
        io::stdin().read_line(&mut choice)
            .expect("Failed to read line");
        let choice: i32 = choice.trim().parse()
            .expect("Please type a number!");
        match choice {
            1 => insert_struct(&no_compression),
            2 => update_struct(&no_compression),
            3 => delete_struct(&no_compression),
            4 => select_struct(&no_compression),
            _ => println!("enter correct choice"),
        }
        println!("do u want to continue:y for yes or n for no");
        let preference=io::stdin().read_line(&mut preference)
            .expect("Failed to read line");
        }
}

#[derive(Clone, Debug, IntoCDRSValue, TryFromRow, PartialEq)]
struct StudentRecord {
    serial_no: i32,
    students: Student,
}

impl StudentRecord {
    fn into_query_values(self) -> QueryValues {
        query_values!(self.serial_no ,self.students)
    }
}

#[derive(Debug, Clone, PartialEq, IntoCDRSValue, TryFromUDT)]
struct Student {
    roll_no: i32,
    name: String,
    marks: i32,
}

fn create_keyspace(session: &CurrentSession) {
    let create_ks: &'static str = "CREATE KEYSPACE IF NOT EXISTS student_ks WITH REPLICATION = { \
                                 'class' : 'SimpleStrategy', 'replication_factor' : 1 };";
    session.query(create_ks).expect("Keyspace creation error");
}

fn create_udt(session: &CurrentSession) {
    let create_type_cql = "CREATE TYPE IF NOT EXISTS student_ks.student (roll_no int,name text,marks int)";
    session
        .query(create_type_cql)
        .expect("udt creation error");
}

fn create_table(session: &CurrentSession) {
    let create_table_cql =
        "CREATE TABLE IF NOT EXISTS student_ks.my_student_table (serial_no int PRIMARY KEY, \
     students frozen <student_ks.student>);";
    session
        .query(create_table_cql)
        .expect("Table creation error");
}

fn take_input() -> Student {
    println!("enter student roll no ");
    let mut student_rollno = String::new();
    io::stdin().read_line(&mut student_rollno)
        .expect("Failed to read line");
    let student_rollno: i32 = student_rollno.trim().parse()
        .expect("Please type a number!");

    println!("enter student name ");
    let mut student_name = String::new();
    io::stdin().read_line(&mut student_name)
        .expect("Failed to read line");

    println!("enter student marks");
    let mut student_marks = String::new();
    io::stdin().read_line(&mut student_marks)
        .expect("Failed to read line");
    let student_marks: i32 = student_marks.trim().parse()
        .expect("Please type a decimal number!");

    let stu: Student = Student {
        roll_no: student_rollno,
        name: student_name,
        marks: student_marks,
    };

    stu
}

fn insert_struct(session: &CurrentSession) {
    let input = take_input();
    let student_record = StudentRecord {
        serial_no: 101,
        students: Student {
            roll_no: input.roll_no,
            name: input.name,
            marks: input.marks,
        },
    };

    let insert_struct_cql = "INSERT INTO student_ks.my_student_table \
                           (serial_no , students) VALUES (?, ?)";
    session
        .query_with_values(insert_struct_cql, student_record.into_query_values())
        .expect("insert");
}

fn select_struct(session: &CurrentSession) {
    let select_struct_cql = "SELECT * FROM student_ks.my_student_table";
    let rows = session
        .query(select_struct_cql)
        .expect("query")
        .get_body()
        .expect("get body")
        .into_rows()
        .expect("into rows");

    for row in rows {
        let my_row: StudentRecord = StudentRecord::try_from_row(row).expect("into StudentRecord");
        println!("struct got: {:?}", my_row);
    }
}

fn update_struct(session: &CurrentSession) {
    let update_struct_cql = "UPDATE student_ks.my_student_table SET students = ? WHERE serial_no = ?";
    let update_input = take_input();
    let upd_student = Student {
        roll_no: update_input.roll_no,
        name: update_input.name,
        marks: update_input.marks,
    };
    let student_serial_no = 101;
    session
        .query_with_values(update_struct_cql, query_values!(upd_student, student_serial_no))
        .expect("update");
}

fn delete_struct(session: &CurrentSession) {
    let delete_struct_cql = "DELETE FROM student_ks.my_student_table WHERE serial_no = ?";
    let student_serial_no = 101;
    session
        .query_with_values(delete_struct_cql, query_values!(student_serial_no))
        .expect("delete");
}