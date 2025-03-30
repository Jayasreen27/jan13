object Second {
  def main(args: Array[String]): Unit = {

//    for (i<-1 to arr.length){
//      if(i%2 ==1){
//        print(arr(i))
//      }
//      else{
//        print(arr(i))
//      }
//      println()
//    }
//val arr=Array(5,15,25,35,45,55,65,75)
//var sum=0
//    for(i<-1 until arr.length ){
//      sum=sum+arr(i)
//    }
//    print(sum)

//    val arr = Array(98, 34, 76, 21, 89, 45, 67, 102)
//    var maxi=arr(0)
//        for(i<-1 until arr.length ){
//          if(arr(i) > maxi){
//            maxi=arr(i)
//                  }
//
//        }
//    print(maxi)

//    val arr = Array(-12, 20, -35, 40, -55, 60, -71, 80)
//    var c_pos=0
//    var c_neg=0
//    for(i<-1 until arr.length ){
//      if (arr(i)<0){
//        c_neg+=1
//      }
//      else{
//        c_pos+=1
//      }
//    }
//    print("postive no count",c_pos)
//    println("Negative no count",c_neg)


    val arr = Array(10, 15, 18, 22, 35, 40, 50, 55)
    for(i<-1 until arr.length ){
      if (arr(i)%5==0){
        arr(i)=arr(i)*2
      }
      else{
        arr(i)=arr(i)+3
      }
    }
print(arr)









  }

}
