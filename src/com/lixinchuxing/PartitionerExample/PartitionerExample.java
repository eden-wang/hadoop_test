package com.lixinchuxing.PartitionerExample;

import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;

public class PartitionerExample extends Configured implements Tool
{
    public static class Bean implements  WritableComparable<Bean> {
        private Text gender;
        private IntWritable age;

        public Bean() {
        }

        public Bean(Text gender, IntWritable age){
            this.gender = gender;
            this.age = age;
        }

        public Text getGender() {
            return gender;
        }

        public void setGender(Text gender) {
            this.gender = gender;
        }


        public IntWritable getAge() {
            return age;
        }

        public void setAge(IntWritable age) {
            this.age = age;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(this.getGender().toString());
            out.writeInt(this.getAge().get());
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.gender = new Text(dataInput.readUTF());
            this.age = new IntWritable(dataInput.readInt());
        }

        @Override
        public int compareTo(Bean bean) {
            int ret = this.gender.toString().compareTo(bean.getGender().toString());
//            if (ret == 0) {
//                return this.getAge().get() - bean.getAge().get();
//            }
            return ret;
        }
    }
    //Map class

    public static class MapClass extends Mapper<LongWritable,Text,Bean,IntWritable>
    {
        public void map(LongWritable key, Text value, Context context)
        {
            try{
                String[] str = value.toString().split("\t");
                String gender=str[3];
                int age = Integer.parseInt(str[2]);

                int salary = Integer.parseInt(str[4]);
                context.write(new Bean(new Text(gender), new IntWritable(age)), new IntWritable(salary));
            }
            catch(Exception e)
            {
                System.out.println(e.getMessage());
            }
        }
    }

    //Reducer class

    public static class ReduceClass extends Reducer<Bean,IntWritable,Text,IntWritable>
    {
        public int max = -1;
        public void reduce(Bean key, Iterable <IntWritable> values, Context context) throws IOException, InterruptedException
        {
            IntWritable max = new IntWritable(-1);

            for (IntWritable val : values)
            {
                if (max.compareTo(val) < 0) {
                    max.set(val.get());
                }
            }

            context.write(key.getGender(), max);
        }
    }

    public static class CaderPartitioner extends
            Partitioner <Bean, IntWritable>
    {
        @Override
        public int getPartition(Bean key, IntWritable value, int numReduceTasks)
        {
            String gender = key.getGender().toString();
            int age = key.getAge().get();

            if(numReduceTasks == 0)
            {
                return 0;
            }

            if(age<=20 && gender.compareTo("Male") == 0)
            {
                return 0;
            }
            else if(age<=20 && gender.compareTo("Female") == 0)
            {
                return 1 % numReduceTasks;
            }
            else if(age>20 && age<=30 && gender.compareTo("Male") == 0)
            {
                return 2 % numReduceTasks;
            }
            else if (age>20 && age<=30 && gender.compareTo("Female") == 0)
            {
                return 3 % numReduceTasks;
            }
            else if (gender.compareTo("Male") == 0)
            {
                return 4 % numReduceTasks;
            }
            else
            {
                return 5 % numReduceTasks;
            }
        }
    }

    @Override
    public int run(String[] arg) throws Exception
    {
        Configuration conf = getConf();

        Job job = new Job(conf, "topsal");
        job.setJarByClass(PartitionerExample.class);

        FileInputFormat.setInputPaths(job, new Path(arg[0]));
        FileOutputFormat.setOutputPath(job,new Path(arg[1]));

        job.setMapperClass(MapClass.class);
        job.setMapOutputKeyClass(Bean.class);
        job.setMapOutputValueClass(IntWritable.class);

        //set partitioner statement

        job.setPartitionerClass(CaderPartitioner.class);
        job.setReducerClass(ReduceClass.class);
        job.setNumReduceTasks(6);
//        job.setInputFormatClass(TextInputFormat.class);

//        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Bean.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true)? 0 : 1);
        return 0;
    }

    public static void main(String ar[]) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new PartitionerExample(),ar);
        System.exit(0);
    }
}