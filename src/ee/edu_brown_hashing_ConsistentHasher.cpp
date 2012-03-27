#include "edu_brown_hashing_ConsistentHasher.h"
#include "conhash.h"

JNIEXPORT jlong JNICALL Java_edu_brown_hashing_ConsistentHasher_nativeCreate
(JNIEnv *env, jobject obj, jint num_partitions){
 	int i;
 	char str[128];
 	struct node_s g_nodes[64];
 	/*init conhash instance*/
 	struct conhash_s *conhash = conhash_init(NULL);
 	if(conhash){
 		for(i = 0; i < num_partitions; i++){
 			sprintf(str, "%d", i);
 			conhash_set_node(&g_nodes[i], str, 1);
 			conhash_add_node(conhash_ &g_nodes[i]);
 		}
	}
	return conhash;

JNIEXPORT jint JNICALL Java_edu_brown_hashing_ConsistentHasher_nativeDestroy
  (JNIEnv *env, jobject obj, jlong conhash){
  	conhash_fini(conhash);
  }


JNIEXPORT jint JNICALL Java_edu_brown_hashing_ConsistentHasher_nativeHashinate
  (JNIEnv *env, jobject obj, jlong conhash, jint value){
  	char str[128];
  	sprintf(str, "%d", value);
 	const struct node_s *node;
 	node = conhash_lookup(conhash, str);
 	return node->iden;
  }



