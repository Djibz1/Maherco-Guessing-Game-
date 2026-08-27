const http = require('http');
const crypto = require('crypto');
const express = require('express');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server);
app.use(express.static(__dirname));

const rooms = new Map();
const MIN_PLAYERS = 4;
const MAX_PLAYERS = 12;
const ROUND_COUNT = 10;

const rounds = [
 {cat:'SPORTS', title:'Top 10 Highest-Paid Athletes', items:['Cristiano Ronaldo','Jon Rahm','Tyson Fury','Lionel Messi','LeBron James','Stephen Curry','Giannis Antetokounmpo','Luka Dončić','Kevin Durant','Patrick Mahomes']},
 {cat:'MOVIES', title:'Top 10 Highest-Grossing Movies', items:['Avatar','Avengers: Endgame','Avatar: The Way of Water','Titanic','Star Wars: The Force Awakens','Avengers: Infinity War','Spider-Man: No Way Home','Ne Zha 2','Inside Out 2','Jurassic World']},
 {cat:'GAMES', title:'Top 10 Best-Selling Video Games', items:['Minecraft','Grand Theft Auto V','Tetris','Wii Sports','PUBG','Mario Kart 8 / Deluxe','Red Dead Redemption 2','Terraria','The Elder Scrolls V: Skyrim','Super Mario Bros.']},
 {cat:'FOOTBALL', title:"Top 10 Ballon d'Or Winners by Wins", items:['Lionel Messi','Cristiano Ronaldo','Michel Platini','Johan Cruyff','Marco van Basten','Franz Beckenbauer','Ronaldo Nazário','Zinédine Zidane','Ronaldinho','Karim Benzema']},
 {cat:'MUSIC', title:'Top 10 Best-Selling Music Artists', items:['The Beatles','Elvis Presley','Michael Jackson','Elton John','Queen','ABBA','Led Zeppelin','Madonna','Pink Floyd','Eagles']},
 {cat:'TECH', title:'Top 10 Most Valuable Companies', items:['Apple','Microsoft','Saudi Aramco','NVIDIA','Alphabet','Amazon','Meta','Berkshire Hathaway','TSMC','Broadcom']},
 {cat:'SPORTS', title:'Top 10 Most Followed Footballers on Instagram', items:['Cristiano Ronaldo','Lionel Messi','Neymar','Kylian Mbappé','Karim Benzema','Marcelo','Ronaldinho','Sergio Ramos','Mohamed Salah','Paul Pogba']},
 {cat:'COUNTRIES', title:'Top 10 Most Populous Countries', items:['India','China','United States','Indonesia','Pakistan','Nigeria','Brazil','Bangladesh','Russia','Ethiopia']},
 {cat:'GENERAL', title:'Top 10 Most Spoken Languages', items:['English','Mandarin Chinese','Hindi','Spanish','French','Arabic','Bengali','Portuguese','Russian','Urdu']},
 {cat:'SPORTS', title:'Top 10 Most Valuable Sports Teams', items:['Dallas Cowboys','Golden State Warriors','Los Angeles Rams','New York Giants','New England Patriots','Los Angeles Lakers','New York Yankees','New York Knicks','Los Angeles Dodgers','San Francisco 49ers']}
];

function code(){return Math.random().toString(36).slice(2,8).toUpperCase();}
function token(){return crypto.randomBytes(18).toString('hex');}
function clamp(n,min,max){return Math.max(min,Math.min(max,n));}
function publicRoom(room, socketId){
  return {
    code:room.code,
    isHost:room.host===socketId || [...room.players.values()].some(p=>p.sessionToken===socketId && room.host===p.sessionToken),
    targetPlayers:room.targetPlayers,
    time:room.time,
    prize:room.prize,
    roundIndex:room.roundIndex,
    remaining:room.remaining,
    started:room.started,
    players:[...room.players.values()].map(p=>({id:p.sessionToken,name:p.name,score:p.score,host:p.sessionToken===room.host,submitted:p.submitted,connected:p.connected!==false}))
  };
}
function broadcast(room,event='lobbyUpdate') { for(const p of room.players.values()) if(p.socketId) io.to(p.socketId).emit(event,publicRoom(room,p.sessionToken)); }
function endRoom(room){ clearInterval(room.timer); clearTimeout(room.nextRoundTimer); rooms.delete(room.code); }

function startRound(room){
  if(!rooms.has(room.code)) return;
  room.started=true;
  room.submissions.clear();
  room.remaining=room.time;
  for(const p of room.players.values()) p.submitted=false;
  io.to(room.code).emit('roundStarted',{...publicRoom(room,null), round:rounds[room.roundIndex]});
  clearInterval(room.timer);
  room.timer=setInterval(()=>{
    room.remaining--;
    io.to(room.code).emit('timerTick',{remaining:room.remaining});
    if(room.remaining<=0){ clearInterval(room.timer); finishRound(room); }
  },1000);
}

function finishRound(room){
  if(!room.started || room.finishing) return;
  room.finishing=true;
  clearInterval(room.timer);
  const correctItems=rounds[room.roundIndex].items;
  for(const [id,sub] of room.submissions){
    const correct=sub.chosen.map((x,i)=>x===correctItems[i]);
    const points=correct.filter(Boolean).length;
    const p=room.players.get(id);
    if(p){p.score+=points;p.lastResult={correct,points};}
  }
  for(const p of room.players.values()) if(!room.submissions.has(p.sessionToken)) p.lastResult={correct:Array(10).fill(false),points:0};
  const submittedIds=[...room.submissions.keys()];
  for(const p of room.players.values()){
    const result=p.lastResult;
    if(p.socketId) io.to(p.socketId).emit('roundSubmitted',{
      title:`الجولة ${room.roundIndex+1}`,
      category:rounds[room.roundIndex].cat,
      items:correctItems,
      correct:result.correct,
      chosen:room.submissions.get(p.sessionToken)?.chosen||[],
      points:result.points,
      allSubmitted:submittedIds.length===room.players.size,
      players:publicRoom(room,null).players,
      roundIndex:room.roundIndex
    });
  }
  room.nextRoundTimer=setTimeout(()=>{
    room.finishing=false;
    room.roundIndex++;
    if(room.roundIndex>=ROUND_COUNT){
      io.to(room.code).emit('finalResults',{players:publicRoom(room,null).players,prize:room.prize});
      endRoom(room);
    } else startRound(room);
  },5000);
}

io.on('connection',socket=>{
  socket.on('createRoom',({name,time,prize,targetPlayers})=>{
    let c; do c=code(); while(rooms.has(c));
    const target=clamp(Number(targetPlayers)||8,MIN_PLAYERS,MAX_PLAYERS);
    const roundTime=clamp(Number(time)||180,30,600);
    const sessionToken=token();
    const room={code:c,host:sessionToken,targetPlayers:target,time:roundTime,prize:String(prize||'0').slice(0,30),players:new Map(),roundIndex:0,remaining:roundTime,started:false,submissions:new Map(),timer:null,nextRoundTimer:null,finishing:false};
    room.players.set(sessionToken,{sessionToken,socketId:socket.id,name:String(name||'Host').trim().slice(0,30),score:0,submitted:false,connected:true});
    rooms.set(c,room);socket.join(c);
    socket.emit('roomCreated',{...publicRoom(room,sessionToken),playerId:sessionToken,sessionToken});
  });

  socket.on('joinRoom',({code,name})=>{
    const room=rooms.get(String(code||'').toUpperCase());
    if(!room)return socket.emit('errorMessage','الغرفة غير موجودة.');
    if(room.started)return socket.emit('errorMessage','اللعبة بدأت بالفعل.');
    if(room.players.size>=room.targetPlayers)return socket.emit('errorMessage',`الغرفة ممتلئة: ${room.targetPlayers} لاعبين.`);
    const clean=String(name||'').trim().slice(0,30);
    if(!clean)return socket.emit('errorMessage','اكتب اسمك.');
    if([...room.players.values()].some(p=>p.name.toLowerCase()===clean.toLowerCase()))return socket.emit('errorMessage','هذا الاسم مستخدم بالفعل.');
    const sessionToken=token();
    room.players.set(sessionToken,{sessionToken,socketId:socket.id,name:clean,score:0,submitted:false,connected:true});socket.join(room.code);
    socket.emit('roomJoined',{...publicRoom(room,sessionToken),playerId:sessionToken,sessionToken});broadcast(room);
  });

  socket.on('rejoinRoom',({code,sessionToken})=>{
    const room=rooms.get(String(code||'').toUpperCase());
    if(!room||!sessionToken)return;
    const p=room.players.get(String(sessionToken));
    if(!p)return;
    p.socketId=socket.id;p.connected=true;socket.join(room.code);
    socket.emit('rejoined',{...publicRoom(room,p.sessionToken),playerId:p.sessionToken,sessionToken:p.sessionToken,round:room.started?rounds[room.roundIndex]:null});
    if(room.started){
      socket.emit('roundStarted',{...publicRoom(room,p.sessionToken),round:rounds[room.roundIndex]});
    } else broadcast(room);
  });

  socket.on('startGame',()=>{
    for(const room of rooms.values()) if(room.host && [...room.players.values()].some(p=>p.sessionToken===room.host&&p.socketId===socket.id)){
      if(room.players.size!==room.targetPlayers)return socket.emit('errorMessage',`نحتاج ${room.targetPlayers} لاعبين لبدء اللعبة.`);
      startRound(room);return;
    }
  });

  socket.on('submitRound',({chosen})=>{
    for(const room of rooms.values()){
      const p=[...room.players.values()].find(x=>x.socketId===socket.id);
      if(p&&room.started&&!room.finishing&&!room.submissions.has(p.sessionToken)){
      p.submitted=true;
      room.submissions.set(p.sessionToken,{chosen:Array.isArray(chosen)?chosen.slice(0,10):[]});
      io.to(room.code).emit('scoresUpdate',{players:publicRoom(room,null).players});
      if(room.submissions.size===room.players.size) finishRound(room);
      return;
      }
    }
  });

  socket.on('disconnect',()=>{
    for(const [code,room] of rooms){
      const p=[...room.players.values()].find(x=>x.socketId===socket.id);if(!p)continue;
      p.connected=false;p.socketId=null;broadcast(room);
      setTimeout(()=>{
        if(!rooms.has(code))return;
        const current=room.players.get(p.sessionToken);
        if(current&&current.connected===false){
          room.players.delete(p.sessionToken);
          if(room.host===p.sessionToken){const next=room.players.values().next().value;room.host=next?.sessionToken||null;}
          if(room.players.size===0){endRoom(room);return;}
          broadcast(room);
          if(room.started&&room.submissions.size===room.players.size)finishRound(room);
        }
      },20000);
    }
  });
});

const PORT=process.env.PORT||3000;
server.listen(PORT,()=>console.log(`Top 10 Multiplayer running on http://localhost:${PORT}`));
