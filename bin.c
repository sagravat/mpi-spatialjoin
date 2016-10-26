#include <stdio.h> 
#include <stdlib.h> 

//function to print the bins: 
void printBins(float bins [500] [500],int items ) 
{ 
    for (int j=1;j<=items;j++) 
    { 
        for (int i=1;i<=items;i++) 
        { 
            printf(" %1.3f",bins[i][j]); 
        } 
        printf("\n"); 
    } 
}

int main() 
{ 
    int items, x=1,c=1, i=0, j=1, countOfBins=1; 
    float z,sum=0 ,inputitems[500], bin[500][500];

    printf("Enter the number of items:"); 
    scanf("%d", &items); //scanning how many elements the user wants to enter 
    if (items<=0) 
    { 
        printf("Can't bin-pack on zero elements\n"); 
        getchar(); 
    } 
    else 
    { 
        do{ 
            printf("\nEnter item number %d: ",x); 
                scanf ("%f",&z); //scanning the elements the user inputs

            if ((z<0)||(z>1)) //assuming my bins range from 0 to 1 only 
            { 
                printf ("\nitem number %d is not in range of 0 to 1\n",x); 
                goto stop; 
            } 
            else //if all the inputs were in the correct range 
            { 
                inputitems[x]=z; //save my elements in an array called inputitems 
                x++; 
            } 
        }while (x<=items); //end of do-while statement


        //technique number 1: 
        printf ("\nIn Next Fit online technique:"); 
        loop: 
            while (c<=items) 
            { 
                if (sum+inputitems[c]<=1) //to check if you have any more room to fit in the bin 
                { 
                    i++; //assuming that each row represents a bin 
                    bin[i][j]=inputitems[c]; //place element from inputitems array into a 2D array called bin 
                    sum += inputitems[c]; 
                    c++; 
                    goto loop; //go back to the while statement to check condition 
                } 
                else 
                { 
                    j++; 
                    countOfBins++; //to count the number of bins we used 
                    i=1; 
                    sum=0; 
                    bin[i][j]=inputitems[c]; 
                    sum += inputitems[c]; 
                    c++; 
                    goto loop; 
                }

            } //end of while loop

            printf("\nNumber of bins used: %d \n", countOfBins); 
            printf("\nstructure of bin\n(FYI any empty rows shown, are equal to the number of items that you have entered)\n"); 
            printBins(bin,items);//to print the bins used with their elements

            //end of first technique, can;t

        stop: getchar(); 
    }//end of very first else

}//end of main



