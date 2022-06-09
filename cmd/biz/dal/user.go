package dal

import (
	"context"
	"errors"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type User struct {
	UserId        int64   `bson:"user_id"`
	Username      string  `bson:"username"`
	FollowCount   int64   `bson:"follow_count"`   // 关注数
	FollowerCount int64   `bson:"follower_count"` // 粉丝数
	Follows       []int64 `bson:"follows"`        // 关注列表
	Followers     []int64 `bson:"followers"`      // 粉丝列表
	PublishList   []int64 `bson:"publish_list"`   // 发布视频列表
	FavoriteList  []int64 `bson:"favorite_list"`  // 点赞列表
}

func ChangeFollowRelation(ctx context.Context, followee int64, follower int64) error {
	userColl := MongoCli.Database("tiktok").Collection("user")

	// 定义事务
	callback := func(sessCtx mongo.SessionContext) (interface{}, error) {
		// 重复关注校验
		err := userColl.FindOne(sessCtx, bson.D{{"user_id", followee}, {"followers", bson.D{{"$all", bson.A{follower}}}}}).Err()
		if err == nil {
			return nil, errors.New("follow again")
		}
		if err != mongo.ErrNoDocuments {
			log.Printf("%v\n", err)
			return nil, err
		}

		filter := bson.D{{"user_id", followee}}
		update := bson.D{
			{"$inc", bson.D{{"follower_count", 1}}},
			{"$addToSet", bson.D{{"followers", follower}}},
		}
		if updateResult, err := userColl.UpdateOne(sessCtx, filter, update); err != nil {
			return nil, err
		} else if updateResult.MatchedCount == 0 {
			return nil, errors.New("no followee was found")
		}
		filter = bson.D{{"user_id", follower}}
		update = bson.D{
			{"$inc", bson.D{{"follow_count", 1}}},
			{"$addToSet", bson.D{{"follows", followee}}},
		}
		if updateResult, err := userColl.UpdateOne(sessCtx, filter, update); err != nil {
			return nil, err
		} else if updateResult.MatchedCount == 0 {
			return nil, errors.New("no follower was found")
		}
		return nil, nil
	}

	// 开启会话
	session, err := MongoCli.StartSession()
	if err != nil {
		log.Printf("ERROR: fail to start mongo session. %v\n", err)
		return err
	}
	defer session.EndSession(ctx)

	// 执行事务
	_, err = session.WithTransaction(ctx, callback)
	if err != nil {
		log.Printf("ERROR: fail to ChangeFollowRelation. %v\n", err)
		return err
	}
	return nil
}

// 赞操作
func FavoriteAction(ctx context.Context, user_id int64, video_id int64, actionType int32) error {
	userColl := MongoCli.Database("tiktok").Collection("user")
	videoColl := MongoCli.Database("tiktok").Collection("video")
	// 定义事务
	callback := func(sessCtx mongo.SessionContext) (interface{}, error) {
		//根据id获取对应的用户和视频数据
		filter := bson.D{{"user_id", user_id}}
		var user User
		err := userColl.FindOne(ctx, filter).Decode(&user)
		if err != nil {
			log.Println(err)
			return nil, errors.New("user_id not exist")
		}
		filter = bson.D{{"video_id", video_id}}
		var video Video
		err = videoColl.FindOne(ctx, filter).Decode(&video)
		if err != nil {
			log.Println(err)
			return nil, errors.New("video_id not exist")
		}

		if actionType==1 {		//点赞
			err = Favorite(ctx, user_id, video_id)
			if err!=nil{
				log.Println(err)
				return nil, errors.New("更新用户点赞列表报错")
			}
		} else { 	//取消点赞
			err = CancelFavorite(ctx, user_id, video_id)
			if err!=nil{
				log.Println(err)
				return nil, errors.New("更新视频点赞列表报错")
			}
		}
		return nil, nil
	}

	// 开启会话
	session, err := MongoCli.StartSession()
	if err != nil {
		log.Printf("ERROR: fail to start mongo session. %v\n", err)
		return err
	}
	defer session.EndSession(ctx)

	// 执行事务
	_, err = session.WithTransaction(ctx, callback)
	if err != nil {
		log.Printf("ERROR: fail to ChangeFollowRelation. %v\n", err)
		return err
	}
	return nil
}

//根据id检索用户数据
func QueryUserByID(ctx context.Context, id int64) (*User, error) {
	collection := MongoCli.Database("tiktok").Collection("user")
	filter := bson.D{{"user_id", id}}
	var result User
	err := collection.FindOne(ctx, filter).Decode(&result)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return &result, nil
}

func QueryUserByToken(ctx context.Context, token string) (*User, error) {
	collection := MongoCli.Database("tiktok").Collection("user")
	filter := bson.D{{"username", token}}

	var result User
	err := collection.FindOne(ctx, filter).Decode(&result)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	return &result, nil
}

//获取用户点赞列表
func GetFavoriteList(ctx context.Context, user_id int64) ([]*Video, error) {
	user, err := QueryUserByID(ctx, user_id)
	if err != nil {
		log.Println(err)
		return []*Video{}, err
	}
	res := []*Video{}
	for i := 0; i < len(user.FavoriteList); i++ {
		item := QueryVideoByID(user.FavoriteList[i])
		res = append(res, &item)
	}
	return res, nil
}

//根据id检索视频
func QueryVideoByID(id int64) Video {
	collection := MongoCli.Database("tiktok").Collection("video")
	filter := bson.D{{"video_id", id}}
	var result Video
	err := collection.FindOne(context.TODO(), filter).Decode(&result)
	if err != nil {
		log.Println(err)
	}
	return result
}

//点赞
func Favorite(ctx context.Context, userId, videoId int64) error {
	collection := MongoCli.Database("tiktok").Collection("user")
	query := bson.M{"user_id": userId}
	update := bson.M{"$push": bson.M{"favorite_list": videoId}}
	_, err := collection.UpdateOne(ctx, query, update)
	if err != nil {
		return err
	}

	collection = MongoCli.Database("tiktok").Collection("video")
	//获取点赞数量
	vdo, err := QueryVideoByVideoId(ctx, videoId)
	if err != nil {
		return err
	}
	count := vdo.FavoriteCount
	//更新点赞列表
	query = bson.M{"video_id": videoId}
	update = bson.M{"$push": bson.M{"favorites": userId}}
	_, err = collection.UpdateOne(ctx, query, update)
	if err != nil {
		return err
	}
	//更新点赞数量
	update = bson.M{"$set": bson.M{"favorite_count": count+1}}
	_, err = collection.UpdateOne(ctx, query, update)
	if err != nil {
		return err
	}
	return nil
}

//取消点赞
func CancelFavorite(ctx context.Context, userId, videoId int64) error {
	collection := MongoCli.Database("tiktok").Collection("user")
	query := bson.M{"user_id": userId}
	update := bson.M{"$pull": bson.M{"favorite_list": videoId}}
	_, err := collection.UpdateOne(ctx, query, update)
	if err != nil {
		return err
	}

	collection = MongoCli.Database("tiktok").Collection("video")
	//获取点赞数量
	vdo, err := QueryVideoByVideoId(ctx, videoId)
	if err != nil {
		return err
	}
	count := vdo.FavoriteCount
	//更新点赞列表
	query = bson.M{"video_id": videoId}
	update = bson.M{"$pull": bson.M{"favorites": userId}}
	_, err = collection.UpdateOne(ctx, query, update)
	if err != nil {
		return err
	}
	//更新点赞数量
	if count>0{
		count--
	}
	update = bson.M{"$set": bson.M{"favorite_count": count}}
	_, err = collection.UpdateOne(ctx, query, update)
	if err != nil {
		return err
	}
	return nil
}
