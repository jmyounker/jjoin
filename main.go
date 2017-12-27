package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/urfave/cli"
)

var version string;

func main() {
	app := cli.NewApp()
	app.Usage = "Join dictionaries from JSON streams."
	app.Version = version

	stdFlags := []cli.Flag{
		cli.StringFlag{
			Name:  "left, l",
			Usage: "Left JSON stream.",
		},
		cli.StringFlag{
			Name:  "right, r",
			Usage: "Right JSON stream.",
		},
		cli.StringFlag{
			Name:  "left-key, lk",
			Usage: "Left stream join key.",
		},
		cli.StringFlag{
			Name:  "right-key, rk",
			Usage: "Right stream join key.",
		},
		cli.StringFlag{
			Name:  "using, u",
			Usage: "Join both streams using this key.",
		},

	}

	app.Commands = []cli.Command{
		{
			Name:   "inner",
			Usage: "Inner join",
			Action: actionInnerJoin,
			Flags: stdFlags,
		},
		{
			Name:   "outer",
			Usage: "Full outer join",
			Action: actionFullOuterJoin,
			Flags: stdFlags,
		},
		{
			Name:   "left-outer",
			Usage: "Left outer join",
			Action: actionLeftOuterJoin,
			Flags: stdFlags,
		},
		{
			Name:   "right-outer",
			Usage: "Right outer join",
			Action: actionRightOuterJoin,
			Flags: stdFlags,
		},
		{
			Name:   "symm-diff",
			Usage: "Symmetric difference",
			Action: actionSymmetricDiff,
			Flags: stdFlags,
		},
		{
			Name:   "subtract",
			Action: actionSubtract,
			Usage:  "Subtract right stream from left stream.",
			Flags: stdFlags,
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func actionInnerJoin(c *cli.Context) error {
	params, err := PopulateJoin(c)
	if err != nil {
		return err
	}
	joined := PerformJoin(params, true, false, false)
	DisplayJoinedPairs(joined)
	return nil
}

func actionFullOuterJoin(c *cli.Context) error {
	params, err := PopulateJoin(c)
	if err != nil {
		return err
	}
	joined := PerformJoin(params, true, true, true)
	DisplayJoinedPairs(joined)
	return nil
}

func actionLeftOuterJoin(c *cli.Context) error {
	params, err := PopulateJoin(c)
	if err != nil {
		return err
	}
	joined := PerformJoin(params, true, true, false)
	DisplayJoinedPairs(joined)
	return nil
}

func actionRightOuterJoin(c *cli.Context) error {
	params, err := PopulateJoin(c)
	if err != nil {
		return err
	}
	joined := PerformJoin(params, true, false, true)
	DisplayJoinedPairs(joined)
	return nil
}

func actionSymmetricDiff(c *cli.Context) error {
	params, err := PopulateJoin(c)
	if err != nil {
		return err
	}
	joined := PerformJoin(params, false, true, true)
	DisplayJoinedPairs(joined)
	return nil
}

func actionSubtract(c *cli.Context) error {
	params, err := PopulateJoin(c)
	if err != nil {
		return err
	}
	joined := PerformJoin(params, false, true, false)
	for _, x := range(joined) {
		b, err := json.MarshalIndent(x.Left, "", "  ")
		if err != nil {
			panic("error rendering JSON")
		}
		fmt.Println(string(b))
	}
	return nil
}

type JoinParams struct {
	Left []interface{}
	Right []interface{}
	LeftKey *Key
	RightKey *Key
}

func PopulateJoin(c *cli.Context) (*JoinParams, error) {
	ls, err := getDataStream("left", c)
	if err != nil {
		return nil, err
	}
	rs, err := getDataStream("right", c)
	if err != nil {
		return nil, err
	}
	lk, rk, err := getKeyOpts(c)
	if err != nil {
		return nil, err
	}
	return &JoinParams{
		Left: ls,
		Right: rs,
		LeftKey: lk,
		RightKey: rk,
	}, nil
}

func getDataStream(streamOpt string, c *cli.Context) ([]interface{}, error) {
	fn := c.String(streamOpt)
	if fn == "" {
		return nil, fmt.Errorf("%s data stream required", streamOpt)
	}
	f, err := os.OpenFile(c.String(streamOpt), os.O_RDONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("cannot read %s", fn)
	}
	return  decodeJsonStream(f)
}

func getKeyOpts(c *cli.Context) (*Key, *Key, error) {
	leftKeyExpr := c.String("left-key")
	rightKeyExpr := c.String("right-key")
	bothKeyExpr := c.String("using")

	if bothKeyExpr == "" && leftKeyExpr == "" && rightKeyExpr == "" {
		return nil, nil, errors.New("keys required");
	}
	if bothKeyExpr != "" {
		if leftKeyExpr != "" || rightKeyExpr != "" {
			return nil, nil, errors.New("using is mutually exclusive with left and right key")
		}
		k, err := KeyFromString(bothKeyExpr)
		if err != nil {
			return nil, nil, err
		}
		return k, k, nil
	}
	if leftKeyExpr == "" || rightKeyExpr == "" {
		return nil, nil, errors.New("both left key and right key are required")
	}
	kl, err := KeyFromString(leftKeyExpr)
	if err != nil {
		return nil, nil, fmt.Errorf("left key error: %s", err)
	}
	kr, err := KeyFromString(rightKeyExpr)
	if err != nil {
		return nil, nil, fmt.Errorf("left key error: %s", err)
	}
	return kl, kr, nil
}

func decodeJsonStream(in *os.File) ([]interface{}, error) {
	dec := json.NewDecoder(in)
	r := []interface{}{}
	for {
		var j interface{}
		if err := dec.Decode(&j); err != nil {
			if err == io.EOF {
				return r, nil
			}
			return nil, err
		}
		r = append(r, j)
	}
	return r, nil
}

func PerformJoin(p *JoinParams, inner,leftOuter,rightOuter bool) []JoinedPair {
	leftKeyed := Filter(p.LeftKey.Exists, p.Left)
	rightKeyed := Filter(p.RightKey.Exists, p.Right)
	leftByKey := PartitionByKey(p.LeftKey.Value, leftKeyed)
	rightByKey := PartitionByKey(p.LeftKey.Value, rightKeyed)
	keys := UnionKeys(leftByKey, rightByKey)
	j := []JoinedPair{}
	for k := range(keys) {
		left, ok := leftByKey[k]
		if !ok {
			left = []interface{}{}
		}
		right, ok := rightByKey[k]
		if !ok {
			right = []interface{}{}
		}
		if inner && len(left) > 0 && len(right) > 0 {
			for _, xl := range (left) {
				for _, xr := range (right) {
					j = append(j, JoinedPair{xl, xr})
				}
			}
		}
		if leftOuter && len(left) > 0 && len(right) == 0 {
			for _, x := range(left) {
				j = append(j, JoinedPair{x, nil})
			}

		}
		if rightOuter && len(left) == 0 && len(right) > 0 {
			for _, x := range(right) {
				j = append(j, JoinedPair{nil, x})
			}
		}
	}
	return j
}

type JoinedPair struct {
	Left interface{}
	Right interface{}
}

func DisplayJoinedPairs(p []JoinedPair)  {
	for _, x := range(p) {
		b, err := json.MarshalIndent(
			map[string]interface{}{
			"left": x.Left,
			"right": x.Right,
		}, "", "  ")
		if err != nil {
			panic("error rendering JSON")
		}
		fmt.Println(string(b))
	}
}

func Filter(key func(interface{})bool, seq []interface{}) []interface{} {
	r := []interface{}{}
	for _, x := range(seq) {
		if key(x) {
			r = append(r, x)
		}
	}
	return r
}

func PartitionByKey(key func(interface{}) interface{}, seq []interface{}) map[interface{}][]interface{} {
	part := map[interface{}][]interface{}{}
	for _, x := range(seq) {
		ks := key(x)
		pk, ok := part[ks]
		if !ok {
			pk = []interface{}{}
		}
		part[ks] = append(pk, x)
	}
	return part
}

func UnionKeys(left map[interface{}][]interface{}, right map[interface{}][]interface{}) map[interface{}]struct{} {
	k := map[interface{}]struct{}{}
	for kl := range(left) {
		k[kl] = struct{}{}
	}
	for kr := range(right) {
		k[kr] = struct{}{}
	}
	return k
}


type Key struct {
	Path []string;
}

func (k Key) Get(d interface{}) (bool, interface{}) {
	c := d
	for _, p := range(k.Path) {
		ok, n := nextDict(p, c)
		if !ok {
			return false, nil
		}
		c = n
	}
	return terminalComponent(c)
}

func nextDict(p string, d interface{}) (bool, interface{}) {
	if d == nil {
		return false, nil
	}
	switch v := d.(type) {
	case map[string]interface{}:
		c, ok := v[p]
		if !ok {
			return false, nil
		}
		return true, c
	default:
		return false, nil
	}
}

func terminalComponent(x interface{}) (bool, interface{}) {
	if x == nil {
		return true, nil
	}
	switch x.(type) {
	case bool, int, float64, string:
		return true, x
	default:
		return false, nil
	}
}

func (k Key) Exists(d interface{}) bool {
	x, _ := k.Get(d)
	return x
}

func (k Key) Value(d interface{}) interface{} {
	_, x := k.Get(d)
	return x
}

func KeyFromString(kp string) (*Key, error) {
	if kp == "." {
		return &Key{ Path: []string{} }, nil
	}
	if strings.HasPrefix(kp, ".") {
		kp = string([]rune(kp)[1:])
	}
	return &Key{ Path: strings.Split(kp, ".") }, nil
}

